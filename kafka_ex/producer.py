from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from kafka import KafkaProducer
import json
import six
import sys


if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves


# 텔레그램 봇 토큰과 단체 채팅방 ID 설정
BOT_TOKEN = 'xxxxxxx'
# 7885985935
GROUP_CHAT_ID = xxxxx

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    #bootstrap_servers='kafka:9092',  # Kafka 서버 IP 및 포트
    api_version=(2, 8, 1),
    max_block_ms=120000,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 형식으로 직렬화
)

def start(update: Update, context):
    """Start 명령어 처리 함수"""
    update.message.reply_text('안녕하세요! 이 봇은 메시지를 수신하고 있습니다.')

def handle_message(update: Update, context):
    """수신한 메시지를 출력하고 Kafka로 전송하는 함수"""
    chat_id = update.message.chat.id
    message_text = update.message.text

    # 콘솔에 수신된 메시지 출력
    print(f"채팅방으로부터 수신된 메시지: {message_text}")

    # 단체 채팅방에서 온 메시지만 Kafka로 전송
    if chat_id == GROUP_CHAT_ID:
        message_data = {'message': message_text}
        
        # Kafka 토픽으로 메시지 전송
        producer.send('testtopic', message_data)
        producer.flush()  # 메시지가 즉시 전송되도록 보장
        

def main():
    """봇을 실행하는 메인 함수"""
    # Updater를 사용하여 봇과 연결
    updater = Updater(BOT_TOKEN, use_context=True)

    print("main executing...")
    # 핸들러 추가
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    # 봇 실행 및 메시지 수신 대기
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
