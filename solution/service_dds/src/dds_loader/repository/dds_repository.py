from typing import Any, Dict, List
from lib.pg import PgConnect

from .objects import H_User, H_Product, H_Category, H_Order, H_Restaurant
from .objects import L_order_user, L_order_product, L_product_category, L_product_restaurant
from .objects import S_order_cost, S_order_status, S_product_names, S_restaurant_names, S_user_names
    
class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
    # ========================================ХАБЫ========================================    
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

    # ========================================ЛИНКИ========================================
    def l_order_user_insert(self, l_order_user: L_order_user) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_user_pk,
                            h_order_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_user_pk)s,
                            %(h_order_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': l_order_user.hk_order_user_pk,
                        'h_user_pk': l_order_user.h_user_pk,
                        'h_order_pk': l_order_user.h_order_pk,
                        'load_dt': l_order_user.load_dt,
                        'load_src': l_order_user.load_src
                    }
                )

    def l_order_product_insert(self, objs: List[L_order_product]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for obj in objs:
                    cur.execute(
                        """
                            INSERT INTO dds.l_order_product(
                                hk_order_product_pk,
                                h_product_pk,
                                h_order_pk,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(hk_order_product_pk)s,
                                %(h_product_pk)s,
                                %(h_order_pk)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (hk_order_product_pk) DO NOTHING;
                        """,
                        {
                            'hk_order_product_pk': obj.hk_order_product_pk,
                            'h_product_pk': obj.h_product_pk,
                            'h_order_pk': obj.h_order_pk,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    )

    def l_product_category_insert(self, objs: List[L_product_category]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for obj in objs:
                    cur.execute(
                        """
                            INSERT INTO dds.l_product_category(
                                hk_product_category_pk,
                                h_product_pk,
                                h_category_pk,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(hk_product_category_pk)s,
                                %(h_product_pk)s,
                                %(h_category_pk)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (hk_product_category_pk) DO NOTHING;
                        """,
                        {
                            'hk_product_category_pk': obj.hk_product_category_pk,
                            'h_product_pk': obj.h_product_pk,
                            'h_category_pk': obj.h_category_pk,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    )

    def l_product_restaurant_insert(self, objs: List[L_product_restaurant]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for obj in objs:
                    cur.execute(
                        """
                            INSERT INTO dds.l_product_restaurant(
                                hk_product_restaurant_pk,
                                h_restaurant_pk,
                                h_product_pk,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(hk_product_restaurant_pk)s,
                                %(h_restaurant_pk)s,
                                %(h_product_pk)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                        """,
                        {
                            'hk_product_restaurant_pk': obj.hk_product_restaurant_pk,
                            'h_restaurant_pk': obj.h_restaurant_pk,
                            'h_product_pk': obj.h_product_pk,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    )

    # ========================================САТЕЛЛИТЫ========================================
    def s_order_cost_insert(self, order_cost: S_order_cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            h_order_pk,
                            cost,
                            payment,
                            load_dt,
                            load_src,
                            hk_order_cost_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_cost_hashdiff)s
                        )
                        ON CONFLICT (hk_order_cost_hashdiff) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order_cost.h_order_pk,
                        'cost': order_cost.cost,
                        'payment': order_cost.payment,
                        'load_dt': order_cost.load_dt,
                        'load_src': order_cost.load_src,
                        'hk_order_cost_hashdiff': order_cost.hk_order_cost_hashdiff
                    }
                )

    def s_order_status_insert(self, order_status: S_order_status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            status,
                            load_dt,
                            load_src,
                            hk_order_status_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_status_hashdiff)s
                        )
                        ON CONFLICT (hk_order_status_hashdiff) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order_status.h_order_pk,
                        'status': order_status.status,
                        'load_dt': order_status.load_dt,
                        'load_src': order_status.load_src,
                        'hk_order_status_hashdiff': order_status.hk_order_status_hashdiff
                    }
                )

    def s_product_names_insert(self, objs: List[S_product_names]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for obj in objs:
                    cur.execute(
                        """
                            INSERT INTO dds.s_product_names(
                                h_product_pk,
                                name,
                                load_dt,
                                load_src,
                                hk_product_names_hashdiff
                            )
                            VALUES(
                                %(h_product_pk)s,
                                %(name)s,
                                %(load_dt)s,
                                %(load_src)s,
                                %(hk_product_names_hashdiff)s
                            )
                            ON CONFLICT (hk_product_names_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_product_pk': obj.h_product_pk,
                            'name': obj.name,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_product_names_hashdiff': obj.hk_product_names_hashdiff
                        }
                    )

    def s_restaurant_names_insert(self, restaurant_names: S_restaurant_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                        )
                        ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': restaurant_names.h_restaurant_pk,
                        'name': restaurant_names.name,
                        'load_dt': restaurant_names.load_dt,
                        'load_src': restaurant_names.load_src,
                        'hk_restaurant_names_hashdiff': restaurant_names.hk_restaurant_names_hashdiff
                    }
                )

    def s_user_names_insert(self, user_names: S_user_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                            h_user_pk,
                            username,
                            userlogin,
                            load_dt,
                            load_src,
                            hk_user_names_hashdiff
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_user_names_hashdiff)s
                        )
                        ON CONFLICT (hk_user_names_hashdiff) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user_names.h_user_pk,
                        'username': user_names.username,
                        'userlogin': user_names.userlogin,
                        'load_dt': user_names.load_dt,
                        'load_src': user_names.load_src,
                        'hk_user_names_hashdiff': user_names.hk_user_names_hashdiff
                    }
                )

