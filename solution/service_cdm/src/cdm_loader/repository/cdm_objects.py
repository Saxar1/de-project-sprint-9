import uuid
from datetime import datetime

from pydantic import BaseModel

class User_Category_Counters(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str
    order_cnt: int

class User_Product_Counters(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    order_cnt: int