import hashlib
import json

import requests
from airflow.models import BaseOperator


class GetSidBrain(BaseOperator):

    def __init__(self, url, login, password, telegram_info, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url_brain = url
        self.login = login
        self.password = hashlib.md5(password).hexdigest()
        self.telegram_info = telegram_info

    def execute(self, context):
        data = {'login': self.login, 'password': self.password}
        response = requests.post(self.url_brain, data=data)
        task_instance = context['task_instance']
        self.telegram_info.check_status_code(response, task_instance.task_id)
        try:
            sid_brain = response.json()['result']
            task_instance.xcom_push(key='sid_brain', value=sid_brain)
        except KeyError:
            task_id = task_instance.task_id
            text = response.text
            self.telegram_info.end_massages_error(
                f'Таск с task_id = {task_id}\nВернул неправильный JSON файл\nОтвет:\n{text}'
            )


class GetUrlToJsonBrain(BaseOperator):
    def __init__(self, target_id, format_, telegram_info, **kwargs) -> None:
        super().__init__(**kwargs)
        self.target_id = target_id
        self.format = format_
        self.telegram_info = telegram_info

    def get_url(self, sid):
        return f'http://api.brain.com.ua/pricelists/{self.target_id}/{self.format}/{sid}?lang=ru'

    def execute(self, context):
        task_instance = context['task_instance']
        sid = task_instance.xcom_pull(key='sid_brain', task_ids='get_sid_brain')
        url = self.get_url(sid)
        response = requests.get(url)
        self.telegram_info.check_status_code(response, task_instance)
        try:
            json_url = response.json()['url']
            task_instance.xcom_push(key='url_json_brain', value=json_url)
        except KeyError:
            task_id = task_instance.task_id
            text = response.text
            self.telegram_info.end_massages_error(
                f'Таск с task_id = {task_id}\nВернул неправильный JSON файл\nОтвет:\n{text}'
            )


class GetJsonBrain(BaseOperator):

    @staticmethod
    def get_path(file_name):
        return f'./data/{file_name}'

    def __init__(self, file_name, telegram_info, **kwargs) -> None:
        super().__init__(**kwargs)
        self.path = self.get_path(file_name)
        self.telegram_info = telegram_info

    def execute(self, context):
        task_instance = context['task_instance']
        url = task_instance.xcom_pull(key='url_json_brain', task_ids="get_url_to_json_brain")
        response = requests.get(url)
        self.telegram_info.check_status_code(response, task_instance.task_id)
        json_brain = response.text
        with open(self.path, 'w', encoding='utf-8') as f:
            f.write(json_brain)
        task_instance.xcom_push(key=f'path_to_json_brain', value=self.path)


class BrainJsonParse(BaseOperator):

    def __init__(self, coefficient, store, **kwargs) -> None:
        super().__init__(**kwargs)
        self.coefficient = coefficient
        self.store = store

    @staticmethod
    def get_vendor_code(product):
        return product['Article'] + '_BR'

    @staticmethod
    def get_usd_currency():
        url = 'https://bank.gov.ua/NBUStatService/v1/statdirectory/dollar_info?json'
        r = requests.get(url)
        status_code = r.status_code
        if status_code == 200:
            dict_ = r.json()
            return dict_[0]["rate"]
        else:
            raise Exception(f'Bad requests currency. Status code: {status_code}')

    def get_price(self, product, usd_currency):
        price_store = product["PriceUSD"]
        brain_retail_price = product["RetailPrice"]
        taksa_price = int(self.coefficient * price_store * usd_currency)
        if taksa_price < brain_retail_price:
            return brain_retail_price
        return taksa_price

    def execute(self, context):
        task_instance = context['task_instance']
        file_path = task_instance.xcom_pull(key='path_to_json_brain', task_ids="get_json_brain")
        with open(file_path, encoding='utf-8') as f:
            brain_json = f.read()
        brain_dict = json.loads(brain_json)
        res_brain_dict = {
            'vendor_code': [],
            "price": []
        }
        usd_currency = self.get_usd_currency()
        for product in brain_dict:
            price = self.get_price(brain_dict[product], usd_currency)
            if price >= 40000:
                continue
            res_brain_dict['vendor_code'].append(self.get_vendor_code(brain_dict[product]))
            res_brain_dict["price"].append(price)
        brain_parse_json = json.dumps(res_brain_dict)
        task_instance.xcom_push(key=f'{self.store}_products', value=brain_parse_json)
