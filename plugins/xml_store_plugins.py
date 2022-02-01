import json

import requests
from airflow.models import BaseOperator
from bs4 import BeautifulSoup


class GetDataStore(BaseOperator):

    @staticmethod
    def get_path(file_name):
        return f'./data/{file_name}'

    def __init__(self, url_store, telegram_info, xml_store, file_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url_store
        self.telegram_info = telegram_info
        self.store = xml_store
        self.path = self.get_path(file_name)

    def execute(self, context):
        task_instance = context['task_instance']
        r = requests.get(self.url)
        self.telegram_info.check_status_code(r, task_instance.task_id)
        r.encoding = 'utf-8'
        with open(self.path, 'w', encoding='utf-8') as f:
            f.write(r.text)
        task_instance.xcom_push(key=f'file_name_{self.store}', value=self.path)


class ParseXmlStore(BaseOperator):

    BAD_OFFER = 'bad offer'

    def __init__(self, telegram_info, store, is_ys=False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.telegram_info = telegram_info
        self.store = store
        self.is_ys = is_ys

    @staticmethod
    def get_currency_id(offer):
        currency_id = offer.find('currencyId').contents[0]
        if currency_id == "UAH":
            return True
        return False

    def get_vendor_code(self, offer):
        if self.is_ys:
            return offer.get('id')
        return offer.find('vendorCode').contents[0]

    def get_offer_line(self, offer):
        vendor_code = self.get_vendor_code(offer)
        price = int(offer.find('price').contents[0])

        if vendor_code is None or price is None:
            return self.BAD_OFFER

        template_line = {
            'vendor_code': vendor_code + f'_{self.store}',
            'price': price
        }
        return template_line

    def get_all_store_product(self, soup):
        offers = list(filter(lambda x: self.get_currency_id(x), soup.find_all('offer')))
        template = {
            'vendor_code': [],
            'price': []
        }
        for offer in offers:
            line = self.get_offer_line(offer)
            if line == self.BAD_OFFER:
                continue
            for key in template:
                template[key].append(line[key])
        return template

    def execute(self, context):
        task_instance = context['task_instance']
        file_name = task_instance.xcom_pull(key=f'file_name_{self.store}', task_ids=f"get_xml_{self.store}_product")
        with open(file_name, encoding='utf-8') as f:
            xml_text = f.read()
        soup_store = BeautifulSoup(xml_text, 'xml')
        dict_soup_store = self.get_all_store_product(soup_store)
        json_store = json.dumps(dict_soup_store)
        task_instance.xcom_push(key=f'{self.store}_products', value=json_store)
