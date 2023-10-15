import uuid
from datetime import datetime
from typing import Any, Dict, List

from .cdm_objects import User_Category_Counters, User_Product_Counters

class OrderCdmBuilder:
    def init(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "dds-service-orders"
        self.order_ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')
    
    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def user_category_counters(self) -> List[User_Category_Counters]:
        user_id = self._dict['user_id']
        product = self._dict['product']
        category = self._dict['category']
        
        counters = []
        
        for cat in category:
            category_id = uuid.UUID(cat['id'])
            category_name = cat['name']
            order_cnt = sum(1 for prod in product if prod['id'] == cat['id'])
            
            counter = User_Category_Counters(
                user_id=user_id,
                category_id=category_id,
                category_name=category_name,
                order_cnt=order_cnt
            )
            
            counters.append(counter)
        
        return counters

    def user_product_counters(self) -> List[User_Product_Counters]:
        user_id = user_id = self._dict['user_id']
        product = self._dict['product']
        
        counters = []
        
        for prod in product:
            product_id = uuid.UUID(prod['id'])
            product_name = prod['name']
            order_cnt = sum(1 for p in product if p['id'] == prod['id'])
            
            counter = User_Product_Counters(
                user_id=user_id,
                product_id=product_id,
                product_name=product_name,
                order_cnt=order_cnt
            )
            
            counters.append(counter)
        
        return counters
