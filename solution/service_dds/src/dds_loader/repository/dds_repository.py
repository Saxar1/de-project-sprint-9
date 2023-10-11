from typing import Any, Dict, List
from lib.pg import PgConnect

from .objects import H_User, H_Product, H_Category, H_Order, H_Restaurant
    
class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        
    def h_user_insert(self, user: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )
                
    def h_product_insert(self, objs: List[H_Product]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for obj in objs:
                    cur.execute(
                        """
                            INSERT INTO dds.h_product(
                                h_product_pk,
                                product_id,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(h_product_pk)s,
                                %(product_id)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (h_product_pk) DO NOTHING;
                        """,
                        {
                            'h_product_pk': obj.h_product_pk,
                            'product_id': obj.product_id,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    ) 
    
    def h_category_insert(self, objs: List[H_Category]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for obj in objs:
                    cur.execute(
                        """
                            INSERT INTO dds.h_category(
                                h_category_pk,
                                category_name,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(h_category_pk)s,
                                %(category_name)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (h_category_pk) DO NOTHING;
                        """,
                        {
                            'h_category_pk': obj.h_category_pk,
                            'category_name': obj.category_name,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    ) 
    
    def h_order_insert(self, order: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order.h_order_pk,
                        'order_id': order.order_id,
                        'order_dt': order.order_dt,
                        'load_dt': order.load_dt,
                        'load_src': order.load_src
                    }
                )

    def h_restaurant_insert(self, restaurant: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': restaurant.h_restaurant_pk,
                        'restaurant_id': restaurant.restaurant_id,
                        'load_dt': restaurant.load_dt,
                        'load_src': restaurant.load_src
                    }
                )