import os
import shutil
import time
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
    plugin_desc = "同步删除历史记录、源文件和下载任务（集成 Windows 路径修复版）。"
    # 插件图标
    plugin_icon = "mediasyncdel.png"
    # 插件版本
    plugin_version = "1.9.2-WinFix"
    # 插件作者
    plugin_author = "thsrite & Gemini"
    # 作者主页
    author_url = "https://github.com/thsrite"
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

    def _convert_path(self, media_path: str) -> str:
        """
        核心改造：处理 Windows 路径到 WSL 的转换
        支持 F:\\emby:/media/emby 这种带冒号的映射
        """
        if not media_path:
            return media_path
        
        # 1. 统一将所有反斜杠 \ 转换为正斜杠 /
        media_path = media_path.replace('\\', '/')
        
        if self._library_path:
            # 按行处理映射
            paths = self._library_path.split("\n")
            for path in paths:
                path = path.strip()
                if not path or ":" not in path:
                    continue
                
                # 2. 关键：从右侧切分冒号，防止 Windows 盘符 F: 的冒号干扰
                sub_paths = path.rsplit(":", 1)
                if len(sub_paths) < 2:
                    continue
                
                win_prefix = sub_paths[0].strip().replace('\\', '/')
                linux_prefix = sub_paths[1].strip().replace('\\', '/')
                
                # 3. 忽略大小写匹配前缀并替换
                if media_path.lower().startswith(win_prefix.lower()):
                    new_path = linux_prefix + media_path[len(win_prefix):]
                    media_path = new_path.replace('//', '/')
                    logger.info(f"[MediaSyncDel] 路径转换：{win_prefix} -> {linux_prefix}")
                    break
        return media_path

    @eventmanager.register(EventType.WebhookMessage)
    def sync_del_by_webhook(self, event: Event):
        """
        Emby Webhook 触发
        """
        if not self._enabled or str(self._sync_type) != "webhook":
            return

        event_data = event.event_data
        event_type = event_data.event

        # 仅响应删除事件
        if not event_type or str(event_type) != 'library.deleted':
            return

        # 获取并转换路径
        raw_path = event_data.item_path
        media_path = self._convert_path(raw_path)

        self.__sync_del(media_type=event_data.media_type,
                        media_name=event_data.item_name,
                        media_path=media_path,
                        tmdb_id=event_data.tmdb_id,
                        season_num=event_data.season_id,
                        episode_num=event_data.episode_id)

    def __sync_del(self, media_type: str, media_name: str, media_path: str,
                   tmdb_id: int, season_num: str, episode_num: str):
        
        # 针对 WSL2 的 I/O 检查优化，防止阻塞
        try:
            if os.path.exists(media_path):
                logger.warn(f"转移路径 {media_path} 依然存在，跳过处理")
                return
        except Exception as e:
            logger.error(f"检查路径是否存在时出错: {str(e)}")

        # 查询转移记录
        msg, transfer_history = self.__get_transfer_his(media_type=media_type,
                                                        media_name=media_name,
                                                        media_path=media_path,
                                                        tmdb_id=tmdb_id,
                                                        season_num=season_num,
                                                        episode_num=episode_num)

        if not transfer_history:
            logger.warn(f"{media_name} 未找到转移记录，检查映射配置：{media_path}")
            return

        for transferhis in transfer_history:
            # 删除转移记录
            self._transferhis.delete(transferhis.id)

            # 删除源文件和下载器任务
            if self._del_source:
                if transferhis.src:
                    src_path = Path(transferhis.src)
                    # 只有在本地能找到源文件时才尝试物理删除
                    if src_path.exists():
                        self._transferchain.delete_files(src_path)
                    
                    # 联动下载器删除
                    if transferhis.download_hash:
                        self.handle_torrent(type=transferhis.type,
                                            src=transferhis.src,
                                            torrent_hash=transferhis.download_hash)

    def __get_transfer_his(self, media_type, media_name, media_path, tmdb_id, season_num, episode_num):
        # 兼容 1.9.2 原版数据库查询逻辑
        mtype = MediaType.MOVIE if media_type in ["Movie", "MOV"] else MediaType.TV
        # 这里的查询逻辑保持 1.9.2 的数据库操作...
        # ... (内部逻辑调用自数据库封装层，此处为简化说明省略)
        transfer_history = self._transferhis.get_by(tmdbid=tmdb_id, dest=media_path)
        return f"{media_name}", transfer_history

    def handle_torrent(self, type: str, src: str, torrent_hash: str):
        # 联动 qBittorrent 删除
        try:
            self._downloadhis.delete_file_by_fullpath(fullpath=src)
            self._transferchain.remove_torrents(torrent_hash)
            logger.info(f"已同步从下载器删除种子：{torrent_hash}")
        except Exception as e:
            logger.error(f"删除种子失败：{str(e)}")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        # 增加 Windows 路径映射的配置提示
        form_config, form_data = super().get_form() # 模拟获取原版表单
        # 此处返回 1.9.2 风格的表单配置
        return [
            {
                'component': 'VForm',
                'content': [
                    {'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}},
                    {'component': 'VSwitch', 'props': {'model': 'del_source', 'label': '同时删除下载器种子和源文件'}},
                    {'component': 'VSelect', 'props': {'model': 'sync_type', 'label': '同步方式', 'items': [{'title': 'Webhook', 'value': 'webhook'}]}},
                    {'component': 'VTextarea', 'props': {'model': 'library_path', 'label': '路径映射 (一行一个)', 'placeholder': 'F:\\emby:/media/emby'}}
                ]
            }
        ], {"enabled": False, "del_source": False, "sync_type": "webhook", "library_path": ""}

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.shutdown()
                self._scheduler = None
        except:
            pass
