import uuid
from datetime import datetime
from typing import Any, Dict, List

from .objects import H_User, H_Product, H_Category, H_Order, H_Restaurant
from .objects import L_order_user, L_order_product, L_product_category, L_product_restaurant
from .objects import S_order_cost, S_order_status, S_product_names, S_restaurant_names, S_user_names

class OrderDdsBuilder:
    def init(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "stg-service-orders"
        self.order_ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')
    
    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    # ========================================ХАБЫ========================================
    def h_user(self) -> H_User:
        user_id = str(self._dict['user']['id'])
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
        restaurant_id = str(self._dict['restaurant']['id'])
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    # ========================================ЛИНКИ========================================
    def l_order_user(self, h_user_pk: uuid.UUID, h_order_pk: uuid.UUID) -> L_order_user:
        return L_order_user(
            hk_order_user_pk=self._uuid(h_order_pk),
            h_user_pk=h_user_pk,
            h_order_pk=h_order_pk,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def l_order_product(self, h_product_pk_list: List[uuid.UUID], h_order_pk: uuid.UUID) -> List[L_order_product]:
        order_product = []
        for h_product_pk in h_product_pk_list:
            order_product.append(
                L_order_product(
                    hk_order_product_pk=self._uuid(h_order_pk),
                    h_product_pk=h_product_pk,
                    h_order_pk=h_order_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return order_product
    
    def l_product_category(self, h_product_pk_list: List[uuid.UUID], h_category_pk_list: List[uuid.UUID]) -> List[L_product_category]:
        product_category = []
        for h_product_pk in h_product_pk_list:
          for h_category_pk in h_category_pk_list:
              product_category.append(
                  L_product_category(
                      hk_product_category_pk=self._uuid(h_product_pk),
                      h_product_pk=h_product_pk,
                      h_category_pk=h_category_pk,
                      load_dt=datetime.utcnow(),
                      load_src=self.source_system
                  )
              )
        return product_category

    def l_product_restaurant(self, h_restaurant_pk: uuid.UUID, h_product_pk_list: List[uuid.UUID]) -> List[L_product_restaurant]:
        product_restaurant = []
        for h_product_pk in h_product_pk_list:
            product_restaurant.append(
                L_product_restaurant(
                    hk_product_restaurant_pk=self._uuid(h_product_pk),
                    h_restaurant_pk=h_restaurant_pk,
                    h_product_pk=h_product_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return product_restaurant
    
    # ========================================САТЕЛЛИТЫ========================================
    def s_order_cost(self, h_order_pk: uuid.UUID) -> S_order_cost:
        cost = self._dict['cost']
        payment = self._dict['payment']
        return S_order_cost(
            h_order_pk=h_order_pk,
            cost=cost,
            payment=payment,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff=self._uuid(h_order_pk)
        )
    
    def s_order_status(self, h_order_pk: uuid.UUID) -> S_order_status:
        status = self._dict['status']
        return S_order_status(
            h_order_pk=h_order_pk,
            status=status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=self._uuid(h_order_pk)
        )
    
    def s_product_names(self, h_product_pk_list: List[uuid.UUID]) -> List[S_product_names]:
        product_names = []
        for product in self._dict["products"]:
            if self._uuid(product["id"]) in h_product_pk_list:
                product_names.append(
                    S_product_names(
                        h_product_pk=self._uuid(product["id"]),
                        name=product["name"],
                        load_dt=datetime.utcnow(),
                        load_src=self.source_system,
                        hk_product_names_hashdiff=self._uuid(product["id"])
                    )
                )
        return product_names

    def s_restaurant_names(self, h_restaurant_pk: uuid.UUID) -> S_restaurant_names:
        name = self._dict['restaurant']['name']
        return S_restaurant_names(
            h_restaurant_pk=h_restaurant_pk,
            name=name,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=self._uuid(h_restaurant_pk)
        )
    
    def s_user_names(self, h_user_pk: uuid.UUID) -> S_user_names:
        username = self._dict['user']['name']
        userlogin = self._dict['user']['login']
        return S_user_names(
            h_user_pk=h_user_pk,
            username=username,
            userlogin=userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff=self._uuid(h_user_pk)
        )
