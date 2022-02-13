import os, logging, pika, json
from telegram import KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)

keyboard = [
    [
        KeyboardButton("/start"),
        KeyboardButton("/help"),
    ]
]
reply_markup = ReplyKeyboardMarkup(keyboard)


def start(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    update.message.reply_markdown_v2(
        fr'Hi {user.mention_markdown_v2()}\!',
        reply_markup=reply_markup,
    )

    # Install docker and run following commands to start rabbitmq
    # docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.9-management
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='orders', exchange_type='fanout')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='orders', queue=queue_name)

    def callback(ch, method, properties, body):
        print(" [x] %r" % body.decode("utf-8"))
        order = json.loads(body.decode("utf-8"))

        update.message.reply_text(
            fr"{order['symbol']}, {order['order_type']}, {order['side']}, {order['amount']}, {order['price']}",
            reply_markup=reply_markup,
        )

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()

def help_command(update: Update, context: CallbackContext) -> None:
    update.message.reply_text('Help!', reply_markup=reply_markup)

def echo(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(update.message.text)

def main() -> None:
    if not os.path.isfile("config.json"):
        print("Please copy the config_example.json to config.json and update API keys within the file.")
        exit(-1)

    with open("config.json", "r") as f:
        config = json.load(f)

    updater = Updater(config["telegram_token"])
    updater.bot.send_message(chat_id=config["telegram_chat_id"], text="I'm a bot, please talk to me!", reply_markup=reply_markup)

    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler("start", start, run_async=True))
    dispatcher.add_handler(CommandHandler("help", help_command))
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, echo))

    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
