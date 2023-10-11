import uuid
from datetime import datetime

from pydantic import BaseModel

# ========================================ХАБЫ========================================
class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str

class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str 

class H_Category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str 

class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: str
    order_dt: datetime
    load_dt: datetime
    load_src: str 

class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str 

# ========================================ЛИНКИ========================================
class L_order_product(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_order_user(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_user_pk: uuid.UUID
    h_order_pk: uuid.UUID 
    load_dt: datetime
    load_src: str

class L_product_category(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_product_restaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str

# ========================================САТЕЛЛИТЫ========================================
class S_order_cost(BaseModel):
    h_order_pk: uuid.UUID 
    cost: float
    payment: float 
    load_dt: datetime
    load_src: str 
    hk_order_cost_hashdiff: uuid.UUID 

class S_order_status(BaseModel):
    h_order_pk: uuid.UUID 
    status: str
    load_dt: datetime
    load_src: str 
    hk_order_status_hashdiff: uuid.UUID 

class S_product_names(BaseModel):
    h_product_pk: uuid.UUID 
    name: str
    load_dt: datetime
    load_src: str 
    hk_product_names_hashdiff: uuid.UUID 

class S_restaurant_names(BaseModel):
    h_restaurant_pk: uuid.UUID 
    name: str
    load_dt: datetime
    load_src: str 
    hk_restaurant_names_hashdiff: uuid.UUID 

class S_user_names(BaseModel):
    h_user_pk: uuid.UUID 
    username: str
    userlogin: str 
    load_dt: datetime
    load_src: str 