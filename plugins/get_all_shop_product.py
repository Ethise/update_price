import json

import requests
from airflow.models import BaseOperator
from bs4 import BeautifulSoup


class GetXmlTaksaProduct(BaseOperator):

    @staticmethod
    def get_path(file_name):
        return f'./data/{file_name}'

    def __init__(self, url, file_name, telegram_info, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.path = self.get_path(file_name)
        self.telegram_info = telegram_info

    def execute(self, context):
        r = requests.get(self.url)
        task_instance = context['task_instance']
        self.telegram_info.check_status_code(r, task_instance.task_id)
        r.encoding = 'utf-8'
        with open(self.path, 'w', encoding='utf-8') as f:
            f.write(r.text)
        task_instance.xcom_push(key='file_name_taksa_products', value=self.path)


class ParseXmlTaksa(BaseOperator):

    BAD_OFFER = 'bad offer'

    def __init__(self, telegram_info, **kwargs) -> None:
        super().__init__(**kwargs)
        self.telegram_info = telegram_info

    @staticmethod
    def get_price(offer):
        price = offer.find('price').contents[0]
        if price is None:
            return None
        res = int(price.split('.')[0])
        return res

    @staticmethod
    def get_available(offer):
        available = offer['available']
        if available == 'true':
            return 5
        elif available == 'false':
            return 0
        return None

    @staticmethod
    def get_vendor_code(offer):
        vendor_code = offer.find('vendorCode').text
        if vendor_code is None or vendor_code == '':
            return None
        return vendor_code

    def get_offer_line(self, offer):
        variant_id = offer.find('variantId').contents[0]
        vendor_code = self.get_vendor_code(offer)
        name = offer.find('model').contents[0]
        price = self.get_price(offer)
        available = self.get_available(offer)

        if any([variant_id is None, vendor_code is None, name is None, price is None, available is None]):
            return self.BAD_OFFER

        template_line = {
            'variant_id': variant_id,
            'vendor_code': vendor_code,
            'name': name,
            'price': price,
            'available': available
        }
        return template_line

    def parse_taksa_product(self, soup):
        offers = soup.find_all('offer')
        template = {
            'variant_id': [],
            'vendor_code': [],
            'name': [],
            'price': [],
            'available': []
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
        file_name = task_instance.xcom_pull(key='file_name_taksa_products', task_ids="get_xml_taksa_product")
        with open(file_name, encoding='utf-8') as f:
            xml_text = f.read()
        soup_taksa = BeautifulSoup(xml_text, 'xml')
        dict_soup_taksa = self.parse_taksa_product(soup_taksa)
        json_taksa = json.dumps(dict_soup_taksa)
        task_instance.xcom_push(key='taksa_products', value=json_taksa)
