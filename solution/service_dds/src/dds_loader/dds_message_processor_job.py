from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from .repository import DdsRepository, OrderDdsBuilder

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")
            
            order = msg['payload']
            
            order_builder = OrderDdsBuilder()
            order_builder.init(order)  

            # |-------------------|
            # | Собираем объекты: |
            # |-------------------|
            # =============Хабы=============
            h_user_object = order_builder.h_user()
            h_product_object = order_builder.h_product()
            h_category_object = order_builder.h_category()  
            h_order_object = order_builder.h_order()  
            h_restaurant_object = order_builder.h_restaurant()

            # =============Линки=============    
            # Собираем атрибуты в словарь с обрабатываемого объекта
            user = h_user_object.dict()
            ord = h_order_object.dict()
            h_product_pks = [h_product.h_product_pk for h_product in h_product_object]
            h_category_pks = [h_category.h_category_pk for h_category in h_category_object]
            rest = h_restaurant_object.dict()

            # Создаем линки: 
            l_order_user_obj = order_builder.l_order_user(user['h_user_pk'], ord['h_order_pk'])  
            l_order_product_obj = order_builder.l_order_product(h_product_pks, ord['h_order_pk']) 
            l_product_category_obj = order_builder.l_product_category(h_product_pks, h_category_pks)
            l_product_restaurant_obj = order_builder.l_product_restaurant(rest['h_restaurant_pk'], h_product_pks)
            
            # =============Сателлиты=============

            # |--------------------------------|
            # | Загружаем данные в PostgreSQL: |
            # |--------------------------------|
            # =============Хабы=============
            self._dds_repository.h_user_insert(h_user_object)
            self._dds_repository.h_product_insert(h_product_object)
            self._dds_repository.h_category_insert(h_category_object)
            self._dds_repository.h_order_insert(h_order_object)
            self._dds_repository.h_restaurant_insert(h_restaurant_object)

            # =============Линки=============
            self._dds_repository.l_order_user_insert(l_order_user_obj)
            self._dds_repository.l_order_product_insert(l_order_product_obj)
            self._dds_repository.l_product_category_insert(l_product_category_obj)
            self._dds_repository.l_product_restaurant_insert(l_product_restaurant_obj)

            # =============Сателлиты=============

            # msg_prod = {"id": str(p.h_product_pk),
            #            "name": p_names[p.h_product_pk],
            #            "category": prod_cats[p.h_product_pk]
            # }

            # self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
