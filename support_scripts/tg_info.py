import requests


class TelegramInfo:

    def __init__(self, token, chat_ids_error, chat_ids_success):
        self.token = token
        self.chat_ids_error = chat_ids_error
        self.chat_ids_success = chat_ids_success

    def get_url_send_message(self):
        return f'https://api.telegram.org/bot{self.token}/sendMessage'

    def send_message(self, text, chat_id):
        send_url = self.get_url_send_message()
        r = requests.post(send_url, data={"chat_id": chat_id, "text": text})
        return r.status_code

    def get_url_send_file(self):
        return f'https://api.telegram.org/bot{self.token}/sendDocument'

    def send_file(self, text, chat_id, file_path):
        send_url = self.get_url_send_file()
        r = requests.post(
            send_url,
            data={"chat_id": chat_id, 'caption': text},
            files={"document": open(file_path, "rb")}
        )
        return r.status_code

    @staticmethod
    def check_bot_broke(status_code):
        if status_code != 200:
            raise Exception("\nВ боте случилась ошибка\n")

    def send_massages_error(self, text):
        for chat_id in self.chat_ids_error:
            message_status_code = self.send_message(text, chat_id)
            self.check_bot_broke(message_status_code)

    def send_massages_success(self, text, file_path):
        for chat_id in self.chat_ids_success:
            print('\n')
            print(self.token)
            print(chat_id)
            print('\n')
            message_status_code = self.send_file(text, chat_id, file_path)
            self.check_bot_broke(message_status_code)

    def check_status_code(self, response, task_id):
        status_code = response.status_code
        if status_code != 200:
            text = response.text
            raise Exception(
                f'\nТаск с task_id = {task_id}\nВернул ответ со статусом: {status_code}\nТекст ответа: {text}\n'
            )
