import os
import re
import time
import datetime
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from apscheduler.schedulers.background import BackgroundScheduler

from app import schemas
from app.chain.transfer import TransferChain
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.db.transferhistory_oper import TransferHistoryOper
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import NotificationType, EventType, MediaType, MediaImageType

class MediaSyncDel(_PluginBase):
    # 插件元数据
    plugin_name = "媒体文件同步删除2"
    plugin_desc = "同步删除历史记录、源文件和下载任务（支持 Windows 盘符路径转换）。"
    plugin_icon = "mediasyncdel.png"
    plugin_version = "2.0"
    plugin_author = "thsrite & Gemini"
    plugin_config_prefix = "mediasyncdel_"
    plugin_order = 9
    auth_level = 1

    # 私有属性
    _scheduler: Optional[BackgroundScheduler] = None
    _enabled = False
    _sync_type: str = ""
    _notify = False
    _del_source = False
    _del_history = False
    _exclude_path = None
    _library_path = None
    _transferchain = None
    _transferhis = None
    _downloadhis = None

    def init_plugin(self, config: dict = None):
        self._transferchain = TransferChain()
        self._transferhis = TransferHistoryOper()
        self._downloadhis = DownloadHistoryOper()

        self.stop_service()

        if config:
            self._enabled = config.get("enabled")
            self._sync_type = config.get("sync_type")
            self._notify = config.get("notify")
            self._del_source = config.get("del_source")
            self._del_history = config.get("del_history")
            self._exclude_path = config.get("exclude_path")
            self._library_path = config.get("library_path")

            if self._del_history:
                self.del_data(key="history")

    def _convert_path(self, media_path: str) -> str:
        """解决 F:\\emby 冒号问题的核心函数"""
        if not media_path:
            return media_path
        # 统一正斜杠
        media_path = media_path.replace('\\', '/')
        if self._library_path:
            for line in self._library_path.split("\n"):
                line = line.strip()
                if not line or ":" not in line:
                    continue
                # 从右侧切割冒号，避开盘符 F:
                parts = line.rsplit(":", 1)
                if len(parts) < 2:
                    continue
                win_pre = parts[0].strip().replace('\\', '/')
                lin_pre = parts[1].strip().replace('\\', '/')
                if media_path.lower().startswith(win_pre.lower()):
                    media_path = lin_pre + media_path[len(win_pre):]
                    return media_path.replace('//', '/')
        return media_path

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """重写配置表单，确保 V2 后台不报错且可配置"""
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'del_source', 'label': '删除源文件'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSelect', 'props': {'model': 'sync_type', 'label': '同步方式', 'items': [{'title': 'Webhook', 'value': 'webhook'}, {'title': '日志', 'value': 'log'}]}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'exclude_path', 'label': '排除路径'}}]},
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VTextarea', 'props': {'model': 'library_path', 'rows': 3, 'label': '路径映射 (支持 F:\\emby 格式)', 'placeholder': 'F:\\emby:/media/emby'}}]}
                        ]
                    }
                ]
            }
        ], {
            "enabled": False, "notify": True, "del_source": False, "sync_type": "webhook",
            "library_path": "", "exclude_path": ""
        }

    @eventmanager.register(EventType.WebhookMessage)
    def sync_del_by_webhook(self, event: Event):
        if not self._enabled or str(self._sync_type) != "webhook":
            return
        data = event.event_data
        if not data.event or str(data.event) != 'library.deleted':
            return
        # 路径转换修复
        media_path = self._convert_path(data.item_path)
        self.__sync_del(media_type=data.media_type, media_name=data.item_name,
                        media_path=media_path, tmdb_id=data.tmdb_id,
                        season_num=data.season_id, episode_num=data.episode_id)

    def __sync_del(self, media_type, media_name, media_path, tmdb_id, season_num, episode_num):
        if not media_type: return
        # 查询转移记录
        history = self._transferhis.get_by(tmdbid=tmdb_id, dest=media_path)
        if not history:
            logger.warn(f"[MediaSyncDel] 未找到转移记录: {media_path}")
            return
        for h in history:
            self._transferhis.delete(h.id)
            if self._del_source:
                if h.src and os.path.exists(h.src):
                    self._transferchain.delete_files(Path(h.src))
                if h.download_hash:
                    self.handle_torrent(h.type, h.src, h.download_hash)

    def handle_torrent(self, type: str, src: str, torrent_hash: str):
        try:
            self._downloadhis.delete_file_by_fullpath(fullpath=src)
            self._transferchain.remove_torrents(torrent_hash)
            logger.info(f"[MediaSyncDel] 已同步删除种子: {torrent_hash}")
        except Exception as e:
            logger.error(f"删除种子失败: {str(e)}")

    def stop_service(self):
        try:
            if self._scheduler and self._scheduler.running:
                self._scheduler.shutdown()
        except: pass

    def get_page(self) -> List[dict]:
        return [{'component': 'div', 'text': 'WinFix 1.9.2 运行中，请在 Emby 配置 Webhook。', 'props': {'class': 'text-center'}}]
