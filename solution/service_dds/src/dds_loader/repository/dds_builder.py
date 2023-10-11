import uuid
from datetime import datetime
from typing import Any, Dict, List

from .objects import H_User, H_Product, H_Category, H_Order, H_Restaurant

class OrderDdsBuilder:
    def init(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "stg-service-orders"
        self.order_ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')
    
    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def h_user(self) -> H_User:
        user_id = str(self._dict['user'])
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_product(self) -> List[H_Product]:
        products = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products
    
    def h_category(self) -> List[H_Category]:
        categories = []
        for cat_dict in self._dict['products']:
            cat_id = cat_dict['id']
            cat_name = cat_dict['category']
            categories.append(
                H_Category(
                    h_category_pk=self._uuid(cat_id),
                    category_name=cat_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return categories

    def h_order(self) -> H_Order:
        order_id = str(self._dict['id'])
        order_dt = self._dict['date']
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=order_dt,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_restaurant(self) -> H_Restaurant:
        restaurant_id = str(self._dict['restaurant'])
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )