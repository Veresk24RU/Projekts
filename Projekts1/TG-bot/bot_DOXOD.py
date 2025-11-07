import telebot
from telebot import types

# Замените YOUR_BOT_TOKEN на ваш настоящий токен
bot = telebot.TeleBot('8459091555:AAETkH-vQE2U7SnCfrOjfAbPsmztDDtojd0')

@bot.message_handler(commands=['start'])
def send_keyboard(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    button = types.KeyboardButton("Запуск анализа портфеля")
    markup.add(button)
    bot.send_message(message.chat.id, "Выберите действие:", reply_markup=markup)

@bot.message_handler(func=lambda message: message.text == "Запуск анализа портфеля")
def handle_portfolio_analysis(message):
    bot.send_message(message.chat.id, "Запускаю анализ портфеля")

bot.polling(none_stop=True, interval=0, timeout=20, skip_pending=False)
