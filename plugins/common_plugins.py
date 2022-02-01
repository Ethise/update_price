import datetime
import json

import pandas as pd
from airflow.models import BaseOperator


class FilterTaksaStoreVC(BaseOperator):

    def __init__(self, store, **kwargs) -> None:
        super().__init__(**kwargs)
        self.store = store

    def execute(self, context):
        task_instance = context['task_instance']
        taksa_json = task_instance.xcom_pull(key='taksa_products', task_ids="parse_xml_taksa")
        taksa_dict_all = json.loads(taksa_json)
        taksa_dict = dict()
        for i in range(len(taksa_dict_all['vendor_code'])):
            if taksa_dict_all['vendor_code'][i][-3:] == f'_{self.store}':
                for key in taksa_dict_all:
                    if key not in taksa_dict:
                        taksa_dict[key] = []
                    taksa_dict[key].append(taksa_dict_all[key][i])
        taksa_dict['available'] = len(taksa_dict['available']) * [0]
        json_filter_taksa = json.dumps(taksa_dict)
        task_instance.xcom_push(key=f'taksa_{self.store}_filter_json', value=json_filter_taksa)


class StoreAvailable(BaseOperator):
    def __init__(self, store, **kwargs) -> None:
        super().__init__(**kwargs)
        self.store = store

    @staticmethod
    def intersection_vendor_code(taksa_dict, store_dict):
        set_vendor_code_taksa = set(taksa_dict['vendor_code'])
        set_vendor_code_store = set(store_dict['vendor_code'])
        return set_vendor_code_taksa.intersection(set_vendor_code_store)

    def execute(self, context):
        task_instance = context['task_instance']
        taksa_json = task_instance.xcom_pull(
            key=f'taksa_{self.store}_filter_json',
            task_ids=f"filter_taksa_{self.store}_vc"
        )
        store_json = task_instance.xcom_pull(key=f'{self.store}_products', task_ids=f"parse_{self.store}")
        store_dict = json.loads(store_json)
        taksa_dict = json.loads(taksa_json)
        intersection_vendor_code = self.intersection_vendor_code(taksa_dict, store_dict)

        for vendor_code in intersection_vendor_code:
            vendor_code_index_taksa = taksa_dict['vendor_code'].index(vendor_code)
            vendor_code_index_store = store_dict['vendor_code'].index(vendor_code)
            taksa_dict['price'][vendor_code_index_taksa] = store_dict['price'][vendor_code_index_store]
            taksa_dict['available'][vendor_code_index_taksa] = 5

        json_res_store = json.dumps(taksa_dict)
        task_instance.xcom_push(key=f'{self.store}_new_price', value=json_res_store)


class StoreJsonToExcel(BaseOperator):

    rename_dict = {
        'variant_id': 'ID варианта',
        'vendor_code': 'Артикул',
        'name': 'Название товара',
        'price': 'Цена продажи',
        'available': 'Остаток'
    }

    @staticmethod
    def get_path(file_name):
        return f'./data/{file_name}'

    def __init__(self, coefficient_price, result_file_name, store, telegram_info, **kwargs) -> None:
        super().__init__(**kwargs)
        self.coefficient_price = coefficient_price
        self.path = self.get_path(result_file_name)
        self.store = store
        self.telegram_info = telegram_info

    def execute(self, context):
        task_instance = context['task_instance']
        store_json = task_instance.xcom_pull(key=f'{self.store}_new_price', task_ids=f"{self.store}_available")
        store_dict = json.loads(store_json)
        store_df = pd.DataFrame.from_dict(store_dict)
        store_df['price'] = store_df['price'].astype(int)
        store_df['price'] = self.coefficient_price * store_df['price']
        store_df['price'] = store_df['price'].astype(int)
        store_df.rename(columns=self.rename_dict, inplace=True)
        store_df.to_excel(self.path, index=False)
        task_instance.xcom_push(key=f'result_{self.store}_file_name', value=self.path)


class GetResultFile(BaseOperator):

    @staticmethod
    def file_path(file_name):
        return f'./data/{file_name}'

    def __init__(self, res_file_name, stores, telegram_info, **kwargs) -> None:
        super().__init__(**kwargs)
        self.res_path = self.file_path(res_file_name)
        self.stores = stores
        self.telegram_info = telegram_info

    def execute(self, context):
        task_instance = context['task_instance']
        paths = [
            task_instance.xcom_pull(
                key=f'result_{store}_file_name',
                task_ids=f"{store}_json_to_excel"
            ) for store in self.stores
        ]

        dfs = [pd.read_excel(path, engine='openpyxl') for path in paths]
        df_res = pd.concat(dfs, ignore_index=True)

        df_res.to_excel(self.res_path, index=False)
        self.telegram_info.send_massages_success(f'Результат {datetime.date.today()}', self.res_path)
