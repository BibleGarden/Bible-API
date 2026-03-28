# about.py
from fastapi import APIRouter
from auth import RequireAPIKey
from models import AboutModel

ABOUT_DATA = {
    "contacts": [
        {
            "id": "telegram",
            "icon": "paperplane.fill",
            "url": "https://t.me/Mandarinka4",
            "sort_order": 1,
            "label": {
                "en": "Message on Telegram",
                "ru": "Написать в Telegram",
                "uk": "Написати в Telegram"
            },
            "subtitle": {
                "en": "@Mandarinka4",
                "ru": "@Mandarinka4",
                "uk": "@Mandarinka4"
            }
        },
        {
            "id": "github",
            "icon": "chevron.left.forwardslash.chevron.right",
            "url": "https://github.com/BibleGarden/",
            "sort_order": 2,
            "label": {
                "en": "GitHub",
                "ru": "GitHub",
                "uk": "GitHub"
            },
            "subtitle": {
                "en": "Source code",
                "ru": "Исходный код",
                "uk": "Вихідний код"
            }
        },
        {
            "id": "website",
            "icon": "globe",
            "url": "https://bible.garden",
            "sort_order": 3,
            "label": {
                "en": "Open website",
                "ru": "Открыть сайт",
                "uk": "Відкрити сайт"
            },
            "subtitle": {
                "en": "https://bible.garden",
                "ru": "https://bible.garden",
                "uk": "https://bible.garden"
            }
        }
    ],
    "about_text": {
        "en": "Bible Garden is a Bible reading app with pauses and the ability to listen to several translations, verse by verse.\n\nWHY THESE LANGUAGES?\n\nI truly hope there will be more over time. For now, this is my minimum set: Russian is my native language, English is the language of international communication, and Ukrainian is a small step toward peace.\n\nTEXTS AND AUDIO\n\nThe app currently uses only texts and audio with a clear legal status that can be used lawfully. If someone is ready to help with obtaining permissions for proprietary texts and audio, we can add many more high-quality translations and narrations. If you believe your copyright is being infringed, please contact me on Telegram and I will review and fix it promptly.\n\nFEEDBACK\n\nIf you find a mistake or have suggestions for improvement, please message me on Telegram.",
        "ru": "Bible Garden — приложение для чтения Библии с паузами, с возможностью прослушивания сразу нескольких переводов, стих за стихом.\n\nПОЧЕМУ ИМЕННО ЭТИ ЯЗЫКИ?\n\nОчень надеюсь, что со временем их станет больше, а пока это мой минимальный набор: русский — родной, английский — язык международного общения, украинский — как маленький шаг к миру.\n\nТЕКСТЫ И ОЗВУЧКИ\n\nВ приложении используются только тексты и озвучки с понятным правовым статусом, которые можно использовать легально. Если кто-то готов помочь с получением разрешений на проприетарные тексты и аудио, можно добавить гораздо больше хороших переводов и озвучек. Если вы считаете, что в приложении нарушены ваши авторские права, пожалуйста, напишите мне в Telegram — я проверю и оперативно исправлю.\n\nОБРАТНАЯ СВЯЗЬ\n\nЕсли вы заметили ошибку или хотите предложить улучшение, напишите мне в Telegram.",
        "uk": "Bible Garden — це додаток для читання Біблії з паузами та можливістю прослуховувати одразу кілька перекладів, вірш за віршем.\n\nЧОМУ САМЕ ЦІ МОВИ?\n\nДуже сподіваюся, що з часом їх стане більше, а поки це мій мінімальний набір: російська — рідна, англійська — мова міжнародного спілкування, українська — як маленький крок до миру.\n\nТЕКСТИ ТА ОЗВУЧЕННЯ\n\nУ додатку зараз використовуються лише тексти й озвучення з чітким правовим статусом, які можна використовувати легально. Якщо хтось готовий допомогти з отриманням дозволів на пропрієтарні тексти й аудіо, можна додати значно більше якісних перекладів та озвучень. Якщо ви вважаєте, що в додатку порушено ваші авторські права, будь ласка, напишіть мені в Telegram — я перевірю і оперативно виправлю.\n\nЗВОРОТНИЙ ЗВ'ЯЗОК\n\nЯкщо ви помітили помилку або хочете запропонувати покращення, напишіть мені в Telegram."
    }
}

router = APIRouter()


@router.get('/about', response_model=AboutModel, operation_id="getAbout", tags=["About"])
def get_about(api_key: str = RequireAPIKey):
    """Информация о проекте / About the project"""
    return ABOUT_DATA
