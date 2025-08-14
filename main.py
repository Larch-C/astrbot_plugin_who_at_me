import asyncio
import os
import pickle
import re
import sqlite3
import time

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import At, Node, Nodes, Plain
from astrbot.api.star import Context, Star, register

plugin_dir = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(plugin_dir, "at_records.db")
RECORD_EXPIRATION_SECONDS = 86400


def setup_database():
    """初始化插件数据库，确保表结构存在。"""
    try:
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS at_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    at_user_id TEXT NOT NULL,
                    sender_id TEXT NOT NULL,
                    sender_name TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    message_chain_pickle BLOB NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_timestamp ON at_records (timestamp);"
            )
        logger.info("插件 'astrobot_plugin_who_at_me' 数据库初始化成功")
    except Exception as e:
        logger.error(f"插件 'astrobot_plugin_who_at_me' 数据库初始化失败: {e}")


setup_database()


@register(
    "astrobot_plugin_who_at_me",
    "长安某",
    "记录并查询@我的消息",
    "2.0.0-nonblocking",
    "https://github.com/zgojin/aastrobot_plugin_who_at_me",
)
class AtRecorderPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.loop = asyncio.get_running_loop()
        logger.info("插件 'astrobot_plugin_who_at_me' 已加载")

    def _db_cleanup_records(self):
        """清理过期的@记录。"""
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cutoff_timestamp = int(time.time()) - RECORD_EXPIRATION_SECONDS
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM at_records WHERE timestamp < ?", (cutoff_timestamp,)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"插件 'astrobot_plugin_who_at_me' 清理旧@记录时出错: {e}")

    def _db_write_records(self, records_to_insert: list):
        """批量写入@记录。"""
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    "INSERT INTO at_records (at_user_id, sender_id, sender_name, group_id, message_chain_pickle, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                    records_to_insert,
                )
                conn.commit()
        except Exception as e:
            logger.error(
                f"插件 'astrobot_plugin_who_at_me' 写入@记录到数据库时出错: {e}"
            )

    def _db_fetch_records(self, user_id: str, group_id: str):
        """获取指定用户的@记录。"""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT sender_id, sender_name, message_chain_pickle FROM at_records WHERE (at_user_id = ? OR at_user_id = 'all') AND group_id = ? ORDER BY timestamp ASC",
                (user_id, group_id),
            )
            return cursor.fetchall()

    def _db_delete_records(self, user_id: str, group_id: str):
        """删除指定用户的@记录。"""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM at_records WHERE at_user_id = ? AND group_id = ?",
                (user_id, group_id),
            )
            conn.commit()


    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def record_at_message(self, event: AstrMessageEvent):
        """监听并记录群聊中的@消息。"""
        if re.fullmatch(r"^(谁艾特我|谁@我|谁@我了)[?？]?$", event.message_str):
            return
        await self.loop.run_in_executor(None, self._db_cleanup_records)

        at_user_ids = [
            str(component.qq)
            for component in event.message_obj.message
            if isinstance(component, At)
        ]

        if not at_user_ids:
            return

        sender_id = event.get_sender_id()
        sender_name = event.get_sender_name()
        group_id = event.get_group_id()
        message_chain_pickle = pickle.dumps(event.message_obj.message)
        timestamp = event.message_obj.timestamp

        records_to_insert = [
            (at_id, sender_id, sender_name, group_id, message_chain_pickle, timestamp)
            for at_id in at_user_ids
        ]

        await self.loop.run_in_executor(None, self._db_write_records, records_to_insert)

    @filter.regex(r"^(谁艾特我|谁@我|谁@我了)[?？]?$")
    async def who_at_me(self, event: AstrMessageEvent):
        """响应用户查询，合并转发所有相关的@消息。"""
        if not event.get_group_id():
            return

        await self.loop.run_in_executor(None, self._db_cleanup_records)

        user_id = event.get_sender_id()
        group_id = event.get_group_id()

        try:
            records = await self.loop.run_in_executor(
                None, self._db_fetch_records, user_id, group_id
            )

            if not records:
                yield event.plain_result("最近24小时内在这个群里没有人@你哦")
                return

            forward_nodes = []
            for sender_id, sender_name, msg_pickle in records:
                original_message_chain = pickle.loads(msg_pickle)
                new_content = []
                if original_message_chain:
                    for i, component in enumerate(original_message_chain):
                        if (
                            i > 0
                            and isinstance(original_message_chain[i - 1], At)
                            and isinstance(component, Plain)
                            and component.text
                        ):
                            cleaned_text = component.text.lstrip(" \t\n:：,，.")
                            new_content.append(Plain(text=f" {cleaned_text}"))
                        else:
                            new_content.append(component)

                node = Node(
                    uin=sender_id,
                    name=sender_name,
                    content=new_content or original_message_chain,
                )
                forward_nodes.append(node)

            yield event.chain_result([Nodes(nodes=forward_nodes)])

            await self.loop.run_in_executor(
                None, self._db_delete_records, user_id, group_id
            )

        except Exception as e:
            logger.error(f"插件 'astrobot_plugin_who_at_me' 查询或发送@记录时出错: {e}")
            yield event.plain_result("处理你的请求时发生了一个内部错误")

    async def terminate(self):
        logger.info("插件 'astrobot_plugin_who_at_me' 已卸载")
