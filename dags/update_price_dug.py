from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable

from plugins.common_plugins import FilterTaksaStoreVC, StoreAvailable, StoreJsonToExcel, GetResultFile
from plugins.get_all_shop_product import GetXmlTaksaProduct, ParseXmlTaksa
from plugins.update_brain import GetSidBrain, GetUrlToJsonBrain, GetJsonBrain, BrainJsonParse
from plugins.xml_store_plugins import GetDataStore, ParseXmlStore
from support_scripts.tg_info import TelegramInfo

import json


def get_variable(name_value):
    return Variable.get(name_value)


DAG_ID = get_variable('dag_id')

# taksaexpress
TAKSA_XML_FILE_NAME = get_variable('taksa_xml_file_name')
URL_TAKSA_PRODUCTS = get_variable('url_taksa_products')

# issa
ISSA = 'IS'
URL_ISSA_PRODUCTS = get_variable('url_issa_products')
ISSA_XML_FILE_NAME = get_variable('issa_xml_file_name')
ISSA_COEFFICIENT_PRICE = float(get_variable('issa_coefficient_price'))
ISSA_RESULT_FILE_NAME = get_variable('issa_result_file_name')

# brain
BRAIN = 'BR'
BASE_URL_BRAIN = get_variable('base_url_brain')
LOGIN_BRAIN = get_variable('login_brain')
PASSWORD_BRAIN = get_variable('password_brain').encode(encoding='UTF-8')
BRAIN_TARGET_ID = int(get_variable('brain_target_id'))
BRAIN_FORMAT_RESPONSE = get_variable('brain_format_response')
BRAIN_JSON_FILE_NAME = get_variable('brain_json_file_name')
BRAIN_COEFFICIENT_PRICE = float(get_variable('brain_coefficient_price'))
BRAIN_RESULT_FILE_NAME = get_variable('brain_result_file_name')

# украшения Светлана
YS = 'YS'
URL_YS_PRODUCTS = get_variable('url_ys_products')
YS_XML_FILE_NAME = get_variable('ys_xml_file_name')
YS_COEFFICIENT_PRICE = float(get_variable('ys_coefficient_price'))
YS_RESULT_FILE_NAME = get_variable('ys_result_file_name')

# результирующий файл
RESULT_FILE_NAME = get_variable('result_file_name')
STORES = [ISSA, YS, BRAIN]

# для вывода в телеграм
TELEGRAM_TOKEN = get_variable('telegram_token')
TELEGRAM_ERROR_IDS = json.loads(get_variable('telegram_error_ids'))
TELEGRAM_SUCCESS_IDS = json.loads(get_variable('telegram_success_ids'))
a = 1
with DAG(
        dag_id=DAG_ID,
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=3),
            "start_date": datetime(2021, 12, 5),
        },
        catchup=False) as f:

    telegram_info = TelegramInfo(
        token=TELEGRAM_TOKEN,
        chat_ids_error=TELEGRAM_ERROR_IDS,
        chat_ids_success=TELEGRAM_SUCCESS_IDS
    )

    get_xml_taksa_product = GetXmlTaksaProduct(
        task_id="get_xml_taksa_product",
        provide_context=True,
        url=URL_TAKSA_PRODUCTS,
        file_name=TAKSA_XML_FILE_NAME,
        telegram_info=telegram_info
    )

    parse_xml_taksa = ParseXmlTaksa(
        task_id="parse_xml_taksa",
        provide_context=True,
        telegram_info=telegram_info
    )

    #######################################################

    get_xml_issa_product = GetDataStore(
        task_id=f"get_xml_{ISSA}_product",
        provide_context=True,
        url_store=URL_ISSA_PRODUCTS,
        telegram_info=telegram_info,
        xml_store=ISSA,
        file_name=ISSA_XML_FILE_NAME
    )

    parse_xml_issa = ParseXmlStore(
        task_id=f"parse_{ISSA}",
        provide_context=True,
        telegram_info=telegram_info,
        store=ISSA
    )

    filter_taksa_issa_vc = FilterTaksaStoreVC(
        task_id=f"filter_taksa_{ISSA}_vc",
        provide_context=True,
        store=ISSA
    )

    issa_available = StoreAvailable(
        task_id=f"{ISSA}_available",
        provide_context=True,
        store=ISSA
    )

    issa_json_to_excel = StoreJsonToExcel(
        task_id=f"{ISSA}_json_to_excel",
        provide_context=True,
        coefficient_price=ISSA_COEFFICIENT_PRICE,
        result_file_name=ISSA_RESULT_FILE_NAME,
        store=ISSA,
        telegram_info=telegram_info
    )

    get_xml_ys_product = GetDataStore(
        task_id=f"get_xml_{YS}_product",
        provide_context=True,
        url_store=URL_YS_PRODUCTS,
        telegram_info=telegram_info,
        xml_store=YS,
        file_name=YS_XML_FILE_NAME
    )

    parse_xml_ys = ParseXmlStore(
        task_id=f"parse_{YS}",
        provide_context=True,
        telegram_info=telegram_info,
        store=YS,
        is_ys=True
    )

    filter_taksa_ys_vc = FilterTaksaStoreVC(
        task_id=f"filter_taksa_{YS}_vc",
        provide_context=True,
        store=YS
    )

    ys_available = StoreAvailable(
        task_id=f"{YS}_available",
        provide_context=True,
        store=YS
    )

    ys_json_to_excel = StoreJsonToExcel(
        task_id=f"{YS}_json_to_excel",
        provide_context=True,
        coefficient_price=YS_COEFFICIENT_PRICE,
        result_file_name=YS_RESULT_FILE_NAME,
        store=YS,
        telegram_info=telegram_info
    )

    get_sid_brain = GetSidBrain(
        task_id="get_sid_brain",
        provide_context=True,
        url=BASE_URL_BRAIN,
        login=LOGIN_BRAIN,
        password=PASSWORD_BRAIN,
        telegram_info=telegram_info
    )

    get_url_to_json_brain = GetUrlToJsonBrain(
        task_id="get_url_to_json_brain",
        provide_context=True,
        target_id=BRAIN_TARGET_ID,
        format_=BRAIN_FORMAT_RESPONSE,
        telegram_info=telegram_info
    )

    get_json_brain = GetJsonBrain(
        task_id="get_json_brain",
        provide_context=True,
        file_name=BRAIN_JSON_FILE_NAME,
        telegram_info=telegram_info
    )

    brain_json_parse = BrainJsonParse(
        task_id=f"parse_{BRAIN}",
        provide_context=True,
        coefficient=BRAIN_COEFFICIENT_PRICE,
        store=BRAIN
    )

    filter_taksa_brain_vc = FilterTaksaStoreVC(
        task_id=f"filter_taksa_{BRAIN}_vc",
        provide_context=True,
        store=BRAIN
    )

    brain_available = StoreAvailable(
        task_id=f"{BRAIN}_available",
        provide_context=True,
        store=BRAIN
    )

    brain_json_to_excel = StoreJsonToExcel(
        task_id=f"{BRAIN}_json_to_excel",
        provide_context=True,
        coefficient_price=1,
        result_file_name=BRAIN_RESULT_FILE_NAME,
        store=BRAIN,
        telegram_info=telegram_info
    )

    get_result_file = GetResultFile(
        task_id="get_result_file",
        provide_context=True,
        res_file_name=RESULT_FILE_NAME,
        stores=STORES,
        telegram_info=telegram_info
    )

    get_xml_taksa_product >> parse_xml_taksa >> [filter_taksa_issa_vc, filter_taksa_ys_vc, filter_taksa_brain_vc]

    get_xml_issa_product >> parse_xml_issa
    [filter_taksa_issa_vc, parse_xml_issa] >> issa_available
    issa_available >> issa_json_to_excel

    get_xml_ys_product >> parse_xml_ys
    [filter_taksa_ys_vc, parse_xml_ys] >> ys_available
    ys_available >> ys_json_to_excel

    get_sid_brain >> get_url_to_json_brain >> get_json_brain >> brain_json_parse
    [brain_json_parse, filter_taksa_brain_vc] >> brain_available >> brain_json_to_excel

    [ys_json_to_excel, issa_json_to_excel, brain_json_to_excel] >> get_result_file
