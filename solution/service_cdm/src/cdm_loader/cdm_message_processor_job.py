from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from .repository import CdmRepository
from .repository import OrderCdmBuilder

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                #  producer: KafkaProducer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        # self._producer = producer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")
            
            order_builder = OrderCdmBuilder()
            order_builder.init(msg) 
            
            # |-------------------|
            # | Собираем объекты: |
            # |-------------------|
            user_category_counters = order_builder.user_category_counters()
            user_product_counters = order_builder.user_product_counters()
            
            # |--------------------------------|
            # | Загружаем данные в PostgreSQL: |
            # |--------------------------------|
            # =============Хабы=============
            self._cdm_repository.user_category_cnt_insert(user_category_counters)
            self._cdm_repository.user_product_cnt_insert(user_product_counters)

            # msg_prod = {
            #     "user_id": str(user['h_user_pk']),
            #     "product": self._format_products(s_product_names_obj),
            #     "category": self._format_categories(h_category_object)
            # }

            # self._producer.produce(msg_prod)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

