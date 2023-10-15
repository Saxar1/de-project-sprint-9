from typing import Any, Dict, List
from lib.pg import PgConnect

from .cdm_objects import User_Category_Counters, User_Product_Counters
    
class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_cnt_insert(self, objs: List[User_Category_Counters]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for obj in objs:
                    cur.execute(
                        """
                            INSERT INTO cdm.user_category_counters (
                                user_id,
                                category_id,
                                category_name,
                                order_cnt
                            )
                            VALUES (
                                %(user_id)s,
                                %(category_id)s,
                                %(category_name)s,
                                %(order_cnt)s
                            )
                            ON CONFLICT (user_id, category_id) DO UPDATE
                            SET order_cnt = cdm.user_category_counters.order_cnt + EXCLUDED.order_cnt;
                        """,
                        {
                            'user_id': obj.user_id,
                            'category_id': obj.category_id,
                            'category_name': obj.category_name,
                            'order_cnt': obj.order_cnt
                        }
                    )

    def user_product_cnt_insert(self, objs: List[User_Product_Counters]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for obj in objs:
                    cur.execute(
                        """
                            INSERT INTO cdm.user_product_counters (
                                user_id,
                                product_id,
                                product_name,
                                order_cnt
                            )
                            VALUES (
                                %(user_id)s,
                                %(product_id)s,
                                %(product_name)s,
                                %(order_cnt)s
                            )
                            ON CONFLICT (user_id, product_id) DO UPDATE
                            SET order_cnt = cdm.user_product_counters.order_cnt + EXCLUDED.order_cnt;
                        """,
                        {
                            'user_id': obj.user_id,
                            'product_id': obj.product_id,
                            'product_name': obj.product_name,
                            'order_cnt': obj.order_cnt
                        }
                    )