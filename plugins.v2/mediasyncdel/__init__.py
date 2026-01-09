import os
import shutil
import time
import re
import datetime
import json
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

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
from app.modules.emby import Emby
from app.modules.jellyfin import Jellyfin


class MediaSyncDel(_PluginBase):
    # 插件名称
    plugin_name = "媒体文件同步删除"
    # 插件描述
    plugin_desc = "同步删除历史记录、源文件和下载任务（Win路径修复+无TMDBID强制删除版）。"
    # 插件图标
    plugin_icon = "mediasyncdel.png"
    # 插件版本
    plugin_version = "1.9.4.ForceFix"
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
    _cron: str = ""
    _notify = False
    _del_source = False
    _del_history = False
    _exclude_path = None
    _library_path = None
    _transferchain = None
    _transferhis = None
    _downloadhis = None
    _default_downloader = None
    _storagechain = None
    _downloader_helper = None

    def init_plugin(self, config: dict = None):
        self._transferchain = TransferChain()
        self._downloader_helper = DownloaderHelper()
        self._transferhis = TransferHistoryOper()
        self._downloadhis = DownloadHistoryOper()
        self._storagechain = StorageChain()

        # 停止现有任务
        self.stop_service()

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._sync_type = config.get("sync_type")
            self._cron = config.get("cron")
            self._notify = config.get("notify")
            self._del_source = config.get("del_source")
            self._del_history = config.get("del_history")
            self._exclude_path = config.get("exclude_path")
            self._library_path = config.get("library_path")

            # 获取默认下载器
            try:
                downloader_services = self._downloader_helper.get_services()
                for downloader_name, downloader_info in downloader_services.items():
                    if downloader_info.config.default:
                        self._default_downloader = downloader_name
            except Exception:
                self._default_downloader = settings.DEFAULT_DOWNLOADER

            # 清理插件历史
            if self._del_history:
                self.del_data(key="history")
                self.update_config({
                    "enabled": self._enabled,
                    "sync_type": self._sync_type,
                    "cron": self._cron,
                    "notify": self._notify,
                    "del_source": self._del_source,
                    "del_history": False,
                    "exclude_path": self._exclude_path,
                    "library_path": self._library_path
                })

    def _convert_path(self, media_path: str) -> str:
        """
        核心改造：处理 Windows 路径到 WSL 的转换
        支持 F:\\emby:/media/emby 这种带盘符冒号的映射
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
                
                # 2. 从右侧切分冒号，防止 Windows 盘符 F: 的冒号干扰
                sub_paths = path.rsplit(":", 1)
                if len(sub_paths) < 2:
                    continue
                
                win_prefix = sub_paths[0].strip().replace('\\', '/')
                linux_prefix = sub_paths[1].strip().replace('\\', '/')
                
                # 3. 忽略大小写匹配前缀并替换
                if media_path.lower().startswith(win_prefix.lower()):
                    new_path = linux_prefix + media_path[len(win_prefix):]
                    new_path = new_path.replace('//', '/')
                    logger.info(f"路径转换成功: {media_path} -> {new_path}")
                    return new_path
        return media_path

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": "删除订阅历史记录"
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
        """
        注册插件公共服务
        """
        if self._enabled and str(self._sync_type) == "log":
            if self._cron:
                return [{
                    "id": "MediaSyncDel",
                    "name": "媒体库同步删除服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.sync_del_by_log,
                    "kwargs": {}
                }]
            else:
                return [{
                    "id": "MediaSyncDel",
                    "name": "媒体库同步删除服务",
                    "trigger": "interval",
                    "func": self.sync_del_by_log,
                    "kwargs": {"minutes": 30}
                }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
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
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'del_history', 'label': '删除历史'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSelect', 'props': {'model': 'sync_type', 'label': '媒体库同步方式', 'items': [{'title': 'Webhook', 'value': 'webhook'}, {'title': '日志', 'value': 'log'}, {'title': 'Scripter X', 'value': 'plugin'}]}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '日志检查周期', 'placeholder': '5位cron表达式，留空自动'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VTextField', 'props': {'model': 'exclude_path', 'label': '排除路径'}}]}
                        ]
                    },
                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VTextarea', 'props': {'model': 'library_path', 'rows': '2', 'label': '媒体库路径映射', 'placeholder': 'F:\\emby:/media/emby (支持Windows盘符)'}}]}]},
                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': '关于路径映射（转移后文件路径）：emby:/data/A.mp4, moviepilot:/mnt/link/A.mp4。路径映射填/data:/mnt/link。'}}]}]}
                ]
            }
        ], {
            "enabled": False, "notify": True, "del_source": False, "del_history": False,
            "library_path": "", "sync_type": "webhook", "cron": "*/30 * * * *", "exclude_path": ""
        }

    def get_page(self) -> List[dict]:
        historys = self.get_data('history')
        if not historys:
            return [{'component': 'div', 'text': '暂无数据', 'props': {'class': 'text-center'}}]
        
        historys = sorted(historys, key=lambda x: x.get('del_time'), reverse=True)
        contents = []
        for history in historys:
            htype = history.get("type")
            title = history.get("title")
            unique = history.get("unique")
            year = history.get("year")
            season = history.get("season")
            episode = history.get("episode")
            image = history.get("image")
            del_time = history.get("del_time")

            sub_contents = [
                {'component': 'VCardText', 'props': {'class': 'pa-0 px-2'}, 'text': f'类型：{htype}'},
                {'component': 'VCardText', 'props': {'class': 'pa-0 px-2'}, 'text': f'标题：{title}'},
                {'component': 'VCardText', 'props': {'class': 'pa-0 px-2'}, 'text': f'时间：{del_time}'}
            ]
            contents.append({
                'component': 'VCard',
                'content': [
                    {"component": "VDialogCloseBtn", "props": {'innerClass': 'absolute top-0 right-0'}, "events": {'click': {'api': 'plugin/MediaSyncDel/delete_history', 'method': 'get', 'params': {'key': unique, 'apikey': settings.API_TOKEN}}}},
                    {'component': 'div', 'props': {'class': 'd-flex justify-space-start flex-nowrap flex-row'}, 'content': [
                        {'component': 'div', 'content': [{'component': 'VImg', 'props': {'src': image, 'height': 120, 'width': 80, 'cover': True}}]},
                        {'component': 'div', 'content': sub_contents}
                    ]}
                ]
            })
        return [{'component': 'div', 'props': {'class': 'grid gap-3 grid-info-card'}, 'content': contents}]

    @eventmanager.register(EventType.WebhookMessage)
    def sync_del_by_webhook(self, event: Event):
        if not self._enabled or str(self._sync_type) != "webhook":
            return

        event_data: schemas.WebhookEventInfo = event.event_data
        event_type = event_data.event

        if not event_type or str(event_type) not in ['library.deleted', 'ItemDeleted']:
            return

        # 1. 转换路径
        media_path = self._convert_path(event_data.item_path)
        media_path = media_path.replace('\\', '/')
        
        tmdb_id = event_data.tmdb_id
        season_num = event_data.season_id
        episode_num = event_data.episode_id
        delete_time = self.format_timestamp(event_data.json_object)

        if self._exclude_path and media_path and any(
                os.path.abspath(media_path).startswith(os.path.abspath(path)) for path in
                self._exclude_path.split(",")):
            logger.info(f"媒体路径 {media_path} 已被排除，暂不处理")
            return

        # 2. 【核心修改】移除 TMDB ID 强制检查
        if not tmdb_id and str(event_data.media_type) != 'Season':
             logger.info(f"未获取到 {event_data.item_name} 的 TMDB ID，将尝试通过路径匹配删除")

        self.__sync_del(media_type=event_data.media_type,
                        media_name=event_data.item_name,
                        media_path=media_path,
                        tmdb_id=tmdb_id,
                        season_num=season_num,
                        episode_num=episode_num,
                        delete_time=delete_time)

    @eventmanager.register(EventType.WebhookMessage)
    def sync_del_by_plugin(self, event: Event):
        if not self._enabled or str(self._sync_type) != "plugin": return
        event_data = event.event_data
        if not event_data.event or str(event_data.event) != 'media_del': return

        item_isvirtual = event_data.item_isvirtual
        if not item_isvirtual: return
        if item_isvirtual == 'True': return

        media_path = self._convert_path(event_data.item_path)
        media_path = media_path.replace('\\', '/')
        
        self.__sync_del(media_type=event_data.item_type,
                        media_name=event_data.item_name,
                        media_path=media_path,
                        tmdb_id=event_data.tmdb_id,
                        season_num=event_data.season_id,
                        episode_num=event_data.episode_id)

    @eventmanager.register(EventType.PluginAction)
    def sync_del(self, event: Event = None):
        if not self._enabled or not event: return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "media_sync_del": return

        media_path = self._convert_path(event_data.get("media_path"))
        
        self.__sync_del(media_type=event_data.get("media_type"),
                        media_name=event_data.get("media_name"),
                        media_path=media_path,
                        tmdb_id=event_data.get("tmdb_id"),
                        season_num=event_data.get("season_num"),
                        episode_num=event_data.get("episode_num"))

    def __sync_del(
        self,
        media_type: str,
        media_name: str,
        media_path: str,
        tmdb_id: int,
        season_num: str,
        episode_num: str,
        delete_time: Optional[str] = None,
    ):
        if not media_type: return

        # 兼容重新整理的场景
        if Path(media_path).exists():
            logger.warn(f"转移路径 {media_path} 未被删除或重新生成，跳过处理")
            return

        # 查询转移记录 (优先尝试标准查询)
        msg, transfer_history = self.__get_transfer_his(media_type=media_type,
                                                        media_name=media_name,
                                                        media_path=media_path,
                                                        tmdb_id=tmdb_id,
                                                        season_num=season_num,
                                                        episode_num=episode_num)

        # 3. 【核心修改】双重兜底查询
        # 如果 TMDB ID 查不到，强制使用路径反查
        if not transfer_history:
            logger.info(f"标准查询未找到 {media_name}，尝试使用路径反查: {media_path}")
            transfer_history = self._transferhis.get_by(dest=media_path)
            if transfer_history:
                msg = f"路径匹配 {media_name}"

        if not transfer_history:
            logger.warn(f"{media_type} {media_name} 未获取到可删除数据，路径：{media_path}")
            return

        if delete_time:
            try:
                latest_his = max(transfer_history, key=lambda x: x.date)
                if delete_time < latest_his.date:
                    logger.warn(f"忽略删除 {msg}，整理时间晚于删除事件")
                    return
            except: pass

        logger.info(f"开始同步删除 {msg}, 匹配到 {len(transfer_history)} 条记录")
        
        # 开始执行删除
        del_torrent_hashs = []
        stop_torrent_hashs = []
        error_cnt = 0
        image = 'https://emby.media/notificationicon.png'
        
        for transferhis in transfer_history:
            # 路径二次核对
            # 修改逻辑：如果路径完全一致，则视为匹配（无视标题差异）；否则再检查标题
            if transferhis.dest != media_path:
                title = transferhis.title
                if title not in media_name:
                    logger.warn(f"记录 {title} 与删除媒体 {media_name} 标题不符，路径不匹配，跳过")
                    continue
                
            image = transferhis.image or image

            # 0. 删除转移记录
            self._transferhis.delete(transferhis.id)

            # 1. 删除源文件和种子
            if self._del_source:
                if transferhis.src:
                    try:
                        if Path(transferhis.src).exists():
                            self._transferchain.delete_files(Path(transferhis.src))
                            self.__remove_parent_dir(Path(transferhis.src))
                    except Exception as e:
                        logger.error(f"源文件删除异常: {e}")

                    # 联动下载器删除
                    if transferhis.download_hash:
                        try:
                            flag, success, hashes = self.handle_torrent(
                                type=transferhis.type,
                                src=transferhis.src,
                                torrent_hash=transferhis.download_hash)
                            if success:
                                if flag: del_torrent_hashs.extend(hashes)
                                else: stop_torrent_hashs.extend(hashes)
                            else:
                                error_cnt += 1
                        except Exception as e:
                            logger.error("删除种子失败：%s" % str(e))

        logger.info(f"同步删除 {msg} 完成！")
        
        media_type_enum = MediaType.MOVIE if media_type in ["Movie", "MOV"] else MediaType.TV

        # 发送消息
        if self._notify:
            backrop_image = self.chain.obtain_specific_image(
                mediaid=tmdb_id,
                mtype=media_type_enum,
                image_type=MediaImageType.Backdrop,
                season=season_num,
                episode=episode_num
            ) or image

            torrent_cnt_msg = ""
            if del_torrent_hashs:
                torrent_cnt_msg += f"删除种子{len(set(del_torrent_hashs))}个\n"
            if stop_torrent_hashs:
                stop_cnt = 0
                for stop_hash in set(stop_torrent_hashs):
                    if stop_hash not in set(del_torrent_hashs):
                        stop_cnt += 1
                if stop_cnt > 0:
                    torrent_cnt_msg += f"暂停种子{stop_cnt}个\n"
            if error_cnt:
                torrent_cnt_msg += f"删种失败{error_cnt}个\n"
            
            self.post_message(
                mtype=NotificationType.MediaServer,
                title="媒体库同步删除任务完成",
                image=backrop_image,
                text=f"{msg}\n"
                     f"删除记录{len(transfer_history)}个\n"
                     f"{torrent_cnt_msg}"
                     f"时间 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}"
            )

        # 记录历史
        history = self.get_data('history') or []
        poster_image = self.chain.obtain_specific_image(
            mediaid=tmdb_id,
            mtype=media_type_enum,
            image_type=MediaImageType.Poster,
        ) or image
        history.append({
            "type": media_type_enum.value,
            "title": media_name,
            "year": None, # 简化
            "path": media_path,
            "season": season_num,
            "episode": episode_num,
            "image": poster_image,
            "del_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
            "unique": f"{media_name}:{tmdb_id}:{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}"
        })
        self.save_data("history", history)

    def __remove_parent_dir(self, file_path: Path):
        if not SystemUtils.exits_files(file_path.parent, settings.RMT_MEDIAEXT):
            i = 0
            for parent_path in file_path.parents:
                i += 1
                if i > 3: break
                if str(parent_path.parent) != str(file_path.root):
                    if not SystemUtils.exits_files(parent_path, settings.RMT_MEDIAEXT):
                        try: shutil.rmtree(parent_path)
                        except: pass
                        logger.warn(f"本地空目录 {parent_path} 已删除")

    def __get_transfer_his(self, media_type: str, media_name: str, media_path: str,
                           tmdb_id: int, season_num: str, episode_num: str):
        if season_num and str(season_num).isdigit():
            season_num = str(season_num).rjust(2, '0')
        else: season_num = None
        if episode_num and str(episode_num).isdigit():
            episode_num = str(episode_num).rjust(2, '0')
        else: episode_num = None

        mtype = MediaType.MOVIE if media_type in ["Movie", "MOV"] else MediaType.TV

        if mtype == MediaType.MOVIE:
            return f'电影 {media_name} {tmdb_id}', self._transferhis.get_by(tmdbid=tmdb_id,
                                                                               mtype=mtype.value,
                                                                               dest=media_path)
        elif mtype == MediaType.TV and not season_num and not episode_num:
            return f'剧集 {media_name} {tmdb_id}', self._transferhis.get_by(tmdbid=tmdb_id,
                                                                               mtype=mtype.value)
        elif mtype == MediaType.TV and season_num and not episode_num:
            if not season_num or not str(season_num).isdigit(): return "", []
            msg = f'剧集 {media_name} S{season_num} {tmdb_id}'
            if tmdb_id and str(tmdb_id).isdigit():
                return msg, self._transferhis.get_by(tmdbid=tmdb_id, mtype=mtype.value, season=f'S{season_num}')
            else:
                return msg, self._transferhis.get_by(mtype=mtype.value, season=f'S{season_num}', dest=media_path)
        elif mtype == MediaType.TV and season_num and episode_num:
            if not season_num or not episode_num: return "", []
            msg = f'剧集 {media_name} S{season_num}E{episode_num} {tmdb_id}'
            return msg, self._transferhis.get_by(tmdbid=tmdb_id, mtype=mtype.value, season=f'S{season_num}', episode=f'E{episode_num}', dest=media_path)
        return "", []

    def sync_del_by_log(self):
        history = self.get_data('history') or []
        last_time = self.get_data("last_time") or None
        del_medias = []
        
        if not settings.MEDIASERVER: return
        for ms in settings.MEDIASERVER.split(','):
            if ms == 'emby': del_medias.extend(self.parse_emby_log(last_time))
            elif ms == 'jellyfin': del_medias.extend(self.parse_jellyfin_log(last_time))

        if not del_medias: return

        for del_media in del_medias:
            media_path = self._convert_path(del_media.get("path"))
            media_path = media_path.replace('\\', '/')
            self.__sync_del(media_type=del_media.get("type"),
                            media_name=del_media.get("name"),
                            media_path=media_path,
                            tmdb_id=None,
                            season_num=del_media.get("season"),
                            episode_num=del_media.get("episode"))
        
        self.save_data("last_time", datetime.datetime.now())

    def handle_torrent(self, type: str, src: str, torrent_hash: str):
        download_id = torrent_hash
        download = self._default_downloader
        history_key = "%s-%s" % (download, torrent_hash)
        plugin_id = "TorrentTransfer"
        transfer_history = self.get_data(key=history_key, plugin_id=plugin_id)
        
        handle_torrent_hashs = []
        try:
            self._downloadhis.delete_file_by_fullpath(fullpath=src)
            download_files = self._downloadhis.get_files_by_hash(download_hash=torrent_hash)
            
            if not download_files: return False, False, 0

            no_del_cnt = 0
            for download_file in download_files:
                if download_file and download_file.state and int(download_file.state) == 1:
                    no_del_cnt += 1

            delete_flag = (no_del_cnt == 0)
            if delete_flag:
                logger.info(f"查询种子任务 {torrent_hash} 文件已全部删除，执行删除种子操作")
            else:
                logger.info(f"查询种子任务 {torrent_hash} 存在 {no_del_cnt} 个未删除文件，执行暂停种子操作")

            if transfer_history and isinstance(transfer_history, dict):
                download = transfer_history['to_download']
                download_id = transfer_history['to_download_id']
                delete_source = transfer_history['delete_source']

                if delete_flag:
                    self.del_data(key=history_key, plugin_id=plugin_id)
                    if not delete_source:
                        self.chain.remove_torrents(torrent_hash)
                        handle_torrent_hashs.append(torrent_hash)
                    self.chain.remove_torrents(hashs=torrent_hash, downloader=download)
                    handle_torrent_hashs.append(download_id)
                else:
                    if not delete_source:
                        self.chain.stop_torrents(torrent_hash)
                        handle_torrent_hashs.append(torrent_hash)
                    self.chain.stop_torrents(hashs=download_id, downloader=download)
                    handle_torrent_hashs.append(download_id)
            else:
                if delete_flag:
                    self.chain.remove_torrents(download_id)
                else:
                    self.chain.stop_torrents(download_id)
                handle_torrent_hashs.append(download_id)

            handle_torrent_hashs = self.__del_seed(download_id=download_id,
                                                   delete_flag=delete_flag,
                                                   handle_torrent_hashs=handle_torrent_hashs)
            if str(type) == "电视剧":
                handle_torrent_hashs = self.__del_collection(src=src,
                                                             delete_flag=delete_flag,
                                                             torrent_hash=torrent_hash,
                                                             download_files=download_files,
                                                             handle_torrent_hashs=handle_torrent_hashs)
            return delete_flag, True, handle_torrent_hashs
        except Exception as e:
            logger.error(f"删种失败： {str(e)}")
            return False, False, 0

    def __del_collection(self, src: str, delete_flag: bool, torrent_hash: str, download_files: list,
                         handle_torrent_hashs: list):
        try:
            src_download_files = self._downloadhis.get_files_by_fullpath(fullpath=src)
            if src_download_files:
                for download_file in src_download_files:
                    if download_file and download_file.download_hash and str(download_file.download_hash) != str(torrent_hash):
                        hash_download_files = self._downloadhis.get_files_by_hash(download_hash=download_file.download_hash)
                        if hash_download_files and len(hash_download_files) > len(download_files) and hash_download_files[0].id > download_files[-1].id:
                            no_del_cnt = 0
                            for hash_download_file in hash_download_files:
                                if hash_download_file and hash_download_file.state and int(hash_download_file.state) == 1:
                                    no_del_cnt += 1
                            if no_del_cnt > 0: delete_flag = False

                            if delete_flag:
                                self.chain.remove_torrents(hashs=download_file.download_hash, downloader=download_file.downloader)
                            else:
                                self.chain.stop_torrents(hashs=download_file.download_hash, downloader=download_file.downloader)
                            handle_torrent_hashs.append(download_file.download_hash)
                            handle_torrent_hashs = self.__del_seed(download_id=download_file.download_hash,
                                                                   delete_flag=delete_flag,
                                                                   handle_torrent_hashs=handle_torrent_hashs)
        except Exception as e:
            logger.error(f"处理 {torrent_hash} 合集失败: {e}")
        return handle_torrent_hashs

    def __del_seed(self, download_id, delete_flag, handle_torrent_hashs):
        history_key = download_id
        plugin_id = "IYUUAutoSeed"
        seed_history = self.get_data(key=history_key, plugin_id=plugin_id) or []
        if seed_history and isinstance(seed_history, list):
            for history in seed_history:
                downloader = history.get("downloader")
                torrents = history.get("torrents")
                if not downloader or not torrents: return
                if not isinstance(torrents, list): torrents = [torrents]
                for torrent in torrents:
                    handle_torrent_hashs.append(torrent)
                    if delete_flag:
                        self.chain.remove_torrents(hashs=torrent, downloader=downloader)
                    else:
                        self.chain.stop_torrents(hashs=torrent, downloader=downloader)
                    handle_torrent_hashs = self.__del_seed(download_id=torrent, delete_flag=delete_flag, handle_torrent_hashs=handle_torrent_hashs)
            if delete_flag:
                self.del_data(key=history_key, plugin_id=plugin_id)
        return handle_torrent_hashs

    @staticmethod
    def parse_emby_log(last_time):
        def __parse_log(file_name: str, del_list: list):
            log_url = f"[HOST]System/Logs/{file_name}?api_key=[APIKEY]"
            log_res = Emby().get_data(log_url)
            if not log_res or log_res.status_code != 200: return del_list
            pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3}) Info App: Removing item from database, Type: (\w+), Name: (.*), Path: (.*), Id: (\d+)'
            matches = re.findall(pattern, log_res.text)
            for match in matches:
                mtime = match[0]
                if last_time and mtime < last_time: continue
                mtype, name, path = match[1], match[2], match[3]
                
                season, episode = None, None
                if mtype == 'Episode' or mtype == 'Season':
                    season_match = re.search(r"Season\s*(\d+)", path)
                    episode_match = re.search(r"S\d+E(\d+)", path)
                    if season_match: season = f'S{str(season_match.group(1)).rjust(2,"0")}'
                    if episode_match: episode = f'E{episode_match.group(1)}'

                del_list.append({"time": mtime, "type": mtype, "name": name, "path": path, "season": season, "episode": episode})
            return del_list

        log_files = []
        try:
            log_list_url = "[HOST]System/Logs/Query?Limit=3&api_key=[APIKEY]"
            log_list_res = Emby().get_data(log_list_url)
            if log_list_res and log_list_res.status_code == 200:
                for item in json.loads(log_list_res.text).get("Items", []):
                    if str(item.get('Name')).startswith("embyserver"):
                        log_files.append(str(item.get('Name')))
        except: pass
        if not log_files: log_files.append("embyserver.txt")
        
        del_medias = []
        for log_file in reversed(log_files):
            del_medias = __parse_log(log_file, del_medias)
        return del_medias

    @staticmethod
    def parse_jellyfin_log(last_time):
        return []

    def get_state(self):
        return self._enabled

    def stop_service(self):
        try:
            if self._scheduler: self._scheduler.shutdown()
        except: pass

    @staticmethod
    def get_tmdbimage_url(path: str, prefix="w500"):
        if not path: return ""
        return f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/{prefix}{path}"

    @staticmethod
    def format_timestamp(json_data: dict) -> str:
        from app.utils.string import StringUtils
        return StringUtils.format_timestamp(StringUtils.str_to_timestamp(json_data.get("UtcTimestamp") or json_data.get("Date")))
