import os
import shutil
import time
import datetime
import re
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

from apscheduler.schedulers.background import BackgroundScheduler

from app import schemas
from app.chain.storage import StorageChain
from app.chain.transfer import TransferChain
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.db.models.transferhistory import TransferHistory
from app.db.transferhistory_oper import TransferHistoryOper
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import NotificationType, EventType, MediaType, MediaImageType
from app.utils.system import SystemUtils


class MediaSyncDel(_PluginBase):
    # 插件名称
    plugin_name = "媒体文件同步删除"
    # 插件描述
    plugin_desc = "同步删除历史记录、源文件和下载任务（已修复Windows路径转换）。"
    # 插件图标
    plugin_icon = "mediasyncdel.png"
    # 插件版本
    plugin_version = "1.9.2.1"
    # 插件作者
    plugin_author = "thsrite & Gemini"
    # 插件配置项ID前缀
    plugin_config_prefix = "mediasyncdel_"
    # 加载顺序
    plugin_order = 9
    # 可使用的用户级别
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

        # 停止现有任务
        self.stop_service()

        # 读取配置
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
        """核心逻辑：处理 F:\\emby 等 Windows 路径"""
        if not media_path:
            return media_path
        
        # 1. 统一将所有反斜杠 \ 转换为正斜杠 /
        media_path = media_path.replace('\\', '/')
        
        if self._library_path:
            paths = self._library_path.split("\n")
            for path in paths:
                path = path.strip()
                if not path or ":" not in path:
                    continue
                
                # 2. 从右侧切割冒号，避开 F: 的冒号
                sub_paths = path.rsplit(":", 1)
                if len(sub_paths) < 2:
                    continue
                
                win_prefix = sub_paths[0].strip().replace('\\', '/')
                linux_prefix = sub_paths[1].strip().replace('\\', '/')
                
                # 3. 匹配并替换
                if media_path.lower().startswith(win_prefix.lower()):
                    media_path = linux_prefix + media_path[len(win_prefix):]
                    media_path = media_path.replace('//', '/')
                    logger.info(f"[MediaSyncDel] 路径映射：{win_prefix} -> {linux_prefix}")
                    break
        return media_path

    def get_state(self) -> bool:
        return self._enabled

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": "删除同步删除历史记录"
            }
        ]

    def delete_history(self, key: str, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        historys = self.get_data('history')
        if not historys:
            return schemas.Response(success=False, message="未找到历史记录")
        historys = [h for h in historys if h.get("unique") != key]
        self.save_data('history', historys)
        return schemas.Response(success=True, message="删除成功")

    def get_service(self) -> List[Dict[str, Any]]:
        # 保持原版服务注册逻辑
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """拼装配置页面"""
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
                    {'component': 'VTextarea', 'props': {'model': 'library_path', 'rows': 2, 'label': '媒体库路径映射', 'placeholder': 'F:\\emby:/media/emby'}},
                ]
            }
        ], {
            "enabled": False, "notify": True, "del_source": False, "sync_type": "webhook",
            "library_path": "", "exclude_path": ""
        }

    def get_page(self) -> List[dict]:
        historys = self.get_data('history')
        if not historys:
            return [{'component': 'div', 'text': '暂无删除数据', 'props': {'class': 'text-center'}}]
        # 简化版页面返回，确保加载
        return [{'component': 'div', 'text': f'当前已有 {len(historys)} 条删除同步记录'}]

    @eventmanager.register(EventType.WebhookMessage)
    def sync_del_by_webhook(self, event: Event):
        if not self._enabled or str(self._sync_type) != "webhook": return
        data = event.event_data
        if not data.event or str(data.event) != 'library.deleted': return
        
        # 路径转换
        media_path = self._convert_path(data.item_path)
        
        self.__sync_del(media_type=data.media_type, media_name=data.item_name,
                        media_path=media_path, tmdb_id=data.tmdb_id,
                        season_num=data.season_id, episode_num=data.episode_id)

    def __sync_del(self, media_type, media_name, media_path, tmdb_id, season_num, episode_num):
        if not media_type: return
        
        # 获取转移记录
        # 注意：此处使用 1.9.2 原版的数据库查询操作
        history = self._transferhis.get_by(tmdbid=tmdb_id, dest=media_path)
        
        if history:
            for h in history:
                self._transferhis.delete(h.id)
                if self._del_source:
                    # 尝试物理删除源文件
                    if h.src and os.path.exists(h.src):
                        self._transferchain.delete_files(Path(h.src))
                    # 联动 qB 删除
                    if h.download_hash:
                        self.handle_torrent(h.type, h.src, h.download_hash)

    def handle_torrent(self, type: str, src: str, torrent_hash: str):
        try:
            self._downloadhis.delete_file_by_fullpath(fullpath=src)
            self._transferchain.remove_torrents(torrent_hash)
            logger.info(f"[MediaSyncDel] 已成功同步删除种子: {torrent_hash}")
        except Exception as e:
            logger.error(f"处理种子失败: {str(e)}")

    def stop_service(self):
        try:
            if self._scheduler and self._scheduler.running:
                self._scheduler.shutdown()
        except: pass
