/**
 * Google Apps Script для загрузки данных из Supabase View ozon_dashboard
 *
 * Установка:
 * 1. Открой Google Таблицу
 * 2. Расширения → Apps Script
 * 3. Вставь этот код
 * 4. Замени SUPABASE_URL и SUPABASE_KEY на свои значения
 * 5. Запусти функцию fetchOzonDashboard()
 *
 * Автоматизация:
 * - Триггеры → Добавить триггер → fetchOzonDashboard → По времени
 */

// ============ НАСТРОЙКИ ============
const SUPABASE_URL = 'https://ujgfjynzdjyagrnsrdkq.supabase.co';
const SUPABASE_KEY = 'YOUR_ANON_KEY_HERE'; // Вставь свой anon key
const VIEW_NAME = 'ozon_dashboard';
const SHEET_NAME = 'Sheet1'; // Название листа в Google Таблице
// ===================================

/**
 * Основная функция - загружает данные из Supabase и записывает в таблицу
 */
function fetchOzonDashboard() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);

  if (!sheet) {
    throw new Error(`Лист "${SHEET_NAME}" не найден`);
  }

  // Запрос к Supabase REST API
  const url = `${SUPABASE_URL}/rest/v1/${VIEW_NAME}?select=*&order=Дата.desc`;

  const options = {
    method: 'GET',
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json'
    },
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const responseCode = response.getResponseCode();

  if (responseCode !== 200) {
    throw new Error(`Ошибка API: ${responseCode} - ${response.getContentText()}`);
  }

  const data = JSON.parse(response.getContentText());

  if (!data || data.length === 0) {
    Logger.log('Нет данных для загрузки');
    return;
  }

  // Заголовки (из первой строки данных)
  const headers = ['Дата', 'sku', 'название', 'Клики', 'Заказы', 'CR', 'Выручка', 'Рекламный бюджет', 'Цена', 'Соинвест', 'дрр'];

  // Очищаем лист
  sheet.clearContents();

  // Записываем заголовки
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

  // Форматируем заголовки
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setBackground('#4285F4');
  headerRange.setFontColor('#FFFFFF');
  headerRange.setFontWeight('bold');

  // Преобразуем данные в массив
  const rows = data.map(row => [
    row['Дата'] || '',
    row['sku'] || '',
    row['название'] || '',
    row['Клики'] || 0,
    row['Заказы'] || 0,
    row['CR'] || 0,
    row['Выручка'] || 0,
    row['Рекламный бюджет'] || 0,
    row['Цена'] || '',
    row['Соинвест'] || '',
    row['дрр'] || 0
  ]);

  // Записываем данные
  if (rows.length > 0) {
    sheet.getRange(2, 1, rows.length, headers.length).setValues(rows);
  }

  // Форматирование
  formatSheet(sheet, rows.length + 1, headers.length);

  Logger.log(`Загружено ${rows.length} строк`);
}

/**
 * Форматирование таблицы
 */
function formatSheet(sheet, lastRow, lastCol) {
  // Автоширина колонок
  for (let i = 1; i <= lastCol; i++) {
    sheet.autoResizeColumn(i);
  }

  // Формат даты (колонка A)
  sheet.getRange(2, 1, lastRow - 1, 1).setNumberFormat('dd.mm.yyyy');

  // Формат чисел
  // Клики, Заказы (колонки D, E)
  sheet.getRange(2, 4, lastRow - 1, 2).setNumberFormat('#,##0');

  // CR, дрр (колонки F, K) - проценты
  sheet.getRange(2, 6, lastRow - 1, 1).setNumberFormat('0.00"%"');
  sheet.getRange(2, 11, lastRow - 1, 1).setNumberFormat('0.00"%"');

  // Выручка, Рекламный бюджет, Цена (колонки G, H, I) - деньги
  sheet.getRange(2, 7, lastRow - 1, 3).setNumberFormat('#,##0.00 ₽');

  // Закрепляем первую строку
  sheet.setFrozenRows(1);
}

/**
 * Загрузка данных за определённый период
 * @param {string} dateFrom - начальная дата (YYYY-MM-DD)
 * @param {string} dateTo - конечная дата (YYYY-MM-DD)
 */
function fetchOzonDashboardByPeriod(dateFrom, dateTo) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);

  if (!sheet) {
    throw new Error(`Лист "${SHEET_NAME}" не найден`);
  }

  // Запрос с фильтром по датам
  const url = `${SUPABASE_URL}/rest/v1/${VIEW_NAME}?select=*&Дата=gte.${dateFrom}&Дата=lte.${dateTo}&order=Дата.desc`;

  const options = {
    method: 'GET',
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json'
    },
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const responseCode = response.getResponseCode();

  if (responseCode !== 200) {
    throw new Error(`Ошибка API: ${responseCode} - ${response.getContentText()}`);
  }

  const data = JSON.parse(response.getContentText());
  Logger.log(`Загружено ${data.length} строк за период ${dateFrom} - ${dateTo}`);

  return data;
}

/**
 * Загрузка данных по конкретному SKU
 * @param {string} sku - артикул товара
 */
function fetchOzonDashboardBySku(sku) {
  const url = `${SUPABASE_URL}/rest/v1/${VIEW_NAME}?select=*&sku=eq.${sku}&order=Дата.desc`;

  const options = {
    method: 'GET',
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json'
    },
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const data = JSON.parse(response.getContentText());

  Logger.log(`Загружено ${data.length} строк для SKU ${sku}`);
  return data;
}

/**
 * Добавляет кнопку в меню таблицы
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Ozon Dashboard')
    .addItem('Обновить данные', 'fetchOzonDashboard')
    .addToUi();
}

/**
 * Создаёт триггер для автоматического обновления каждый день в 8:00
 */
function createDailyTrigger() {
  // Удаляем старые триггеры
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === 'fetchOzonDashboard') {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  // Создаём новый триггер
  ScriptApp.newTrigger('fetchOzonDashboard')
    .timeBased()
    .everyDays(1)
    .atHour(8)
    .create();

  Logger.log('Триггер создан: ежедневно в 8:00');
}
