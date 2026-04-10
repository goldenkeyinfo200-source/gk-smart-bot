# Golden Key Railway Bot

Railway учун тайёр Telegram бот лойиҳаси.

## Файллар
- `main.py` — асосий бот
- `config.py` — env конфиг
- `requirements.txt` — Python пакетлар
- `Procfile` — Railway start command
- `railway.json` — Railway healthcheck ва deploy config
- `.env.example` — env намунаси

## Google Sheets варақлари
Автоматик яратилади:
- `Users`
- `Objects`
- `Leads`
- `Settings`

## Асосий функциялар
- `/start` орқали рўйхатдан ўтиш
- мижоз / агент / админ роллари
- агент бўлишга сўров юбориш ва админ тасдиғи
- объект қўшиш (админ ва агент)
- объект қидириш
- мижоздан заявка қабул қилиш
- агентларга янги лид юбориш
- `Олдим`, `Рад этдим`, `Бажарилди` тугмалари
- махсус агент referral link: `/ref`
- мижоз referral link орқали кирса, lead ўша махсус агентга боғланади
- lead якунланса махсус агентга бонус ҳақида хабар юборилади
- Railway healthcheck `/health`

## Railway deploy
1. GitHub’га шу файлларни юкланг.
2. Railway’da **New Project -> Deploy from GitHub**.
3. Variables бўлимида `.env.example` ичидаги env’ларни тўлдиринг.
4. Deploy қилинг.

## Эслатма
- `GOOGLE_SERVICE_ACCOUNT_JSON` бир қаторда JSON кўринишида киритилади.
- Google service account email’га Spreadsheet’ни Editor қилиб улаш шарт.
