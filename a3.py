#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FTP Analyzer - Этап 3: Расширенный отчет с показателями продаж
Автор: Система аналитики данных
Версия: 3.0
"""

import ftplib
import os
import json
import sys
import csv
import io
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

class FTPAnalyzer:
    def __init__(self):
        # Настройки FTP-сервера
        self.ftp_host = "smkft.space"
        self.ftp_port = 2122
        self.ftp_user = "nielsen"
        self.ftp_pass = "Qazwsx32123"
        
        # Настройки городов
        self.cities = {
            'khar': 'Харьков',
            'kiev': 'Киев', 
            'dnepr': 'Днепр',
            'bel': 'Белая Церковь'
        }
        
        # Настройки локальных папок
        self.base_dir = Path("ftp_data")
        self.cache_dir = self.base_dir / "cache"
        self.reports_dir = self.base_dir / "reports"
        self.structure_file = self.base_dir / "ftp_structure.json"
        
        # Создаем необходимые папки
        self.create_directories()
        
    def create_directories(self):
        """Создает необходимые папки для работы программы"""
        try:
            self.base_dir.mkdir(exist_ok=True)
            self.cache_dir.mkdir(exist_ok=True)
            self.reports_dir.mkdir(exist_ok=True)
            print(f"✓ Создана рабочая папка: {self.base_dir.absolute()}")
            print(f"✓ Создана папка кеша: {self.cache_dir.absolute()}")
            print(f"✓ Создана папка отчетов: {self.reports_dir.absolute()}")
        except Exception as e:
            print(f"✗ Ошибка создания папок: {e}")
            sys.exit(1)
            
    def clear_cache(self):
        """Очищает кеш"""
        try:
            import shutil
            if self.cache_dir.exists():
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir()
                print("✓ Кеш очищен")
                return True
        except Exception as e:
            print(f"✗ Ошибка очистки кеша: {e}")
            return False
            
    def ask_clear_cache(self):
        """Спрашивает пользователя об очистке кеша"""
        while True:
            choice = input("\n🗑️  Очистить кеш перед загрузкой? (y/n): ").lower().strip()
            if choice in ['y', 'yes', 'д', 'да']:
                return self.clear_cache()
            elif choice in ['n', 'no', 'н', 'нет']:
                print("ℹ️  Кеш сохранен")
                return True
            else:
                print("❌ Введите 'y' для да или 'n' для нет")
                
    def connect_ftp(self):
        """Подключается к FTP-серверу"""
        try:
            print(f"🔄 Подключаемся к FTP-серверу {self.ftp_host}:{self.ftp_port}...")
            ftp = ftplib.FTP()
            ftp.connect(self.ftp_host, self.ftp_port)
            ftp.login(self.ftp_user, self.ftp_pass)
            print("✓ Подключение к FTP установлено")
            return ftp
        except ftplib.error_perm as e:
            print(f"✗ Ошибка авторизации FTP: {e}")
            return None
        except Exception as e:
            print(f"✗ Ошибка подключения к FTP: {e}")
            return None
            
    def download_file(self, ftp, remote_path, local_path):
        """Скачивает файл с FTP сервера"""
        try:
            # Проверяем, есть ли файл в кеше
            if local_path.exists():
                print(f"📄 Файл найден в кеше: {local_path.name}")
                return True
                
            print(f"⬇️  Скачиваем: {remote_path}")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {remote_path}', f.write)
            
            print(f"✓ Скачан: {local_path.name}")
            return True
            
        except ftplib.error_perm as e:
            if "550" in str(e):  # Файл не найден
                print(f"⚠️  Файл не найден: {remote_path}")
            else:
                print(f"✗ Ошибка FTP при скачивании {remote_path}: {e}")
            return False
        except Exception as e:
            print(f"✗ Ошибка скачивания {remote_path}: {e}")
            return False
            
    def read_csv_with_pipe(self, file_path):
        """Читает CSV файл с разделителем |"""
        try:
            df = pd.read_csv(file_path, sep='|', encoding='utf-8')
            
            # Преобразуем числовые поля в правильные типы
            file_name = file_path.name.lower()
            
            if 'cartitem' in file_name:
                # Для файлов cartitem преобразуем qty и total_price в числа
                if 'qty' in df.columns:
                    # Обрабатываем числа с запятой как разделителем десятичных
                    df['qty'] = df['qty'].astype(str).str.replace(',', '.', regex=False)
                    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
                if 'total_price' in df.columns:
                    # Обрабатываем числа с запятой как разделителем десятичных
                    df['total_price'] = df['total_price'].astype(str).str.replace(',', '.', regex=False)
                    df['total_price'] = pd.to_numeric(df['total_price'], errors='coerce').fillna(0.0)
            
            # Убираем пробелы из строковых полей
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()
            
            print(f"✓ Прочитан CSV: {file_path.name} ({len(df)} строк)")
            return df
        except Exception as e:
            print(f"✗ Ошибка чтения CSV {file_path}: {e}")
            return None
            
    def generate_date_range(self, start_date, end_date):
        """Генерирует список дат в указанном диапазоне"""
        dates = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
            
        return dates
        
    def download_shop_files(self, ftp):
        """Скачивает справочники магазинов"""
        print("\n📋 ЗАГРУЗКА СПРАВОЧНИКОВ МАГАЗИНОВ")
        print("-" * 40)
        
        shops_data = {}
        
        for city_code, city_name in self.cities.items():
            remote_path = f"/www/shop_{city_code}.csv"
            local_path = self.cache_dir / f"shop_{city_code}.csv"
            
            if self.download_file(ftp, remote_path, local_path):
                df = self.read_csv_with_pipe(local_path)
                if df is not None:
                    shops_data[city_code] = df
                    print(f"  📊 {city_name}: {len(df)} магазинов")
                    
        return shops_data
        
    def download_receipt_files(self, ftp, start_date, end_date):
        """Скачивает файлы чеков за указанный период"""
        print(f"\n🧾 ЗАГРУЗКА ФАЙЛОВ ЧЕКОВ ({start_date} - {end_date})")
        print("-" * 50)
        
        dates = self.generate_date_range(start_date, end_date)
        receipts_data = {}
        
        for city_code, city_name in self.cities.items():
            print(f"\n🏙️  {city_name} ({city_code}):")
            receipts_data[city_code] = []
            
            for date in dates:
                remote_path = f"/www/receipt/receipt_{city_code}_{date}.csv"
                local_path = self.cache_dir / f"receipt_{city_code}_{date}.csv"
                
                if self.download_file(ftp, remote_path, local_path):
                    df = self.read_csv_with_pipe(local_path)
                    if df is not None:
                        df['date'] = date
                        receipts_data[city_code].append(df)
                        print(f"    📅 {date}: {len(df)} чеков")
                        
        return receipts_data
        
    def download_cartitem_files(self, ftp, start_date, end_date):
        """Скачивает файлы товарных позиций за указанный период"""
        print(f"\n🛒 ЗАГРУЗКА ФАЙЛОВ ТОВАРНЫХ ПОЗИЦИЙ ({start_date} - {end_date})")
        print("-" * 60)
        
        dates = self.generate_date_range(start_date, end_date)
        cartitems_data = {}
        
        for city_code, city_name in self.cities.items():
            print(f"\n🏙️  {city_name} ({city_code}):")
            cartitems_data[city_code] = []
            
            for date in dates:
                remote_path = f"/www/cartitem/cartitem_{city_code}_{date}.csv"
                local_path = self.cache_dir / f"cartitem_{city_code}_{date}.csv"
                
                if self.download_file(ftp, remote_path, local_path):
                    df = self.read_csv_with_pipe(local_path)
                    if df is not None:
                        df['date'] = date
                        cartitems_data[city_code].append(df)
                        total_qty = df['qty'].sum() if 'qty' in df.columns else 0
                        total_price = df['total_price'].sum() if 'total_price' in df.columns else 0
                        print(f"    📅 {date}: {len(df)} позиций, товаров: {float(total_qty):.1f}, оборот: {float(total_price):.2f}")
                        
        return cartitems_data
        
    def consolidate_data_with_sales(self, shops_data, receipts_data, cartitems_data):
        """Консолидирует данные с показателями продаж и создает отчет"""
        print("\n📊 КОНСОЛИДАЦИЯ ДАННЫХ С ПОКАЗАТЕЛЯМИ ПРОДАЖ")
        print("-" * 50)
        
        report_data = []
        
        for city_code, city_name in self.cities.items():
            print(f"\n🏙️  Обрабатываем {city_name}...")
            
            if city_code not in shops_data:
                print(f"  ⚠️  Нет данных о магазинах для {city_name}")
                continue
                
            shops_df = shops_data[city_code]
            
            if city_code not in receipts_data or not receipts_data[city_code]:
                print(f"  ⚠️  Нет данных о чеках для {city_name}")
                # Добавляем магазины с нулевыми показателями
                for _, shop in shops_df.iterrows():
                    report_data.append({
                        'Город': city_name,
                        'ID_магазина': shop['id'],
                        'Название_магазина': shop['name'],
                        'Количество_чеков': 0,
                        'Оборот': 0.0,
                        'Количество_товаров': 0.0
                    })
                continue
                
            # Объединяем все чеки по городу
            all_receipts = pd.concat(receipts_data[city_code], ignore_index=True)
            
            # Считаем количество чеков по каждому магазину
            receipt_counts = all_receipts.groupby('shop_id').size().reset_index(name='receipt_count')
            
            # Обрабатываем товарные позиции
            sales_summary = None
            if city_code in cartitems_data and cartitems_data[city_code]:
                print(f"  🛒 Обрабатываем товарные позиции...")
                
                # Объединяем все товарные позиции по городу
                all_cartitems = pd.concat(cartitems_data[city_code], ignore_index=True)
                
                # Связываем товарные позиции с чеками
                cartitems_with_shops = all_cartitems.merge(
                    all_receipts[['id', 'shop_id']], 
                    left_on='receipt_id', 
                    right_on='id', 
                    how='inner'
                )
                
                if not cartitems_with_shops.empty:
                    # Группируем по магазинам и считаем показатели
                    sales_summary = cartitems_with_shops.groupby('shop_id').agg({
                        'total_price': 'sum',
                        'qty': 'sum'
                    }).reset_index()
                    sales_summary.columns = ['shop_id', 'total_revenue', 'total_quantity']
                    
                    # Убеждаемся что данные в правильном формате
                    sales_summary['total_revenue'] = pd.to_numeric(sales_summary['total_revenue'], errors='coerce').fillna(0.0)
                    sales_summary['total_quantity'] = pd.to_numeric(sales_summary['total_quantity'], errors='coerce').fillna(0.0)
                    
                    print(f"    ✓ Обработано товарных позиций: {len(all_cartitems)}")
                    print(f"    ✓ Связано с чеками: {len(cartitems_with_shops)}")
                else:
                    print(f"    ⚠️  Не удалось связать товарные позиции с чеками")
            else:
                print(f"  ⚠️  Нет данных о товарных позициях для {city_name}")
            
            # Объединяем все данные с информацией о магазинах
            merged = shops_df.merge(receipt_counts, left_on='id', right_on='shop_id', how='left')
            merged['receipt_count'] = merged['receipt_count'].fillna(0).astype(int)
            
            # Добавляем данные о продажах
            if sales_summary is not None:
                merged = merged.merge(sales_summary, left_on='id', right_on='shop_id', how='left')
                merged['total_revenue'] = merged['total_revenue'].fillna(0).round(2)
                merged['total_quantity'] = merged['total_quantity'].fillna(0).astype(int)
            else:
                merged['total_revenue'] = 0.0
                merged['total_quantity'] = 0
            
            # Добавляем в итоговый отчет
            for _, row in merged.iterrows():
                # Убеждаемся что числовые поля в правильном формате
                revenue = float(row['total_revenue']) if pd.notna(row['total_revenue']) else 0.0
                quantity = float(row['total_quantity']) if pd.notna(row['total_quantity']) else 0.0
                
                report_data.append({
                    'Город': city_name,
                    'ID_магазина': row['id'],
                    'Название_магазина': row['name'],
                    'Количество_чеков': int(row['receipt_count']),
                    'Оборот': revenue,
                    'Количество_товаров': quantity
                })
                
            total_receipts = len(all_receipts)
            total_shops = len(shops_df)
            total_revenue = float(merged['total_revenue'].sum()) if 'total_revenue' in merged.columns else 0.0
            total_quantity = float(merged['total_quantity'].sum()) if 'total_quantity' in merged.columns else 0.0
            
            print(f"  ✓ Магазинов: {total_shops}")
            print(f"  ✓ Чеков: {total_receipts}")
            print(f"  ✓ Оборот: {total_revenue:,.2f}")
            print(f"  ✓ Товаров: {total_quantity:,.1f}")
            
        return report_data
        
    def save_excel_report(self, report_data, start_date, end_date):
        """Сохраняет отчет только в Excel"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Создаем DataFrame
        df = pd.DataFrame(report_data)
        
        # Сортируем по городу и обороту
        df = df.sort_values(['Город', 'Оборот'], ascending=[True, False])
        
        # Форматируем числовые поля
        df['Оборот'] = pd.to_numeric(df['Оборот'], errors='coerce').fillna(0.0).round(2)
        df['Количество_товаров'] = pd.to_numeric(df['Количество_товаров'], errors='coerce').fillna(0.0).round(1)
        df['Количество_чеков'] = pd.to_numeric(df['Количество_чеков'], errors='coerce').fillna(0).astype(int)
        
        # Сохраняем в Excel
        excel_file = self.reports_dir / f"sales_report_{start_date}_{end_date}_{timestamp}.xlsx"
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Отчет по продажам', index=False)
            
            # Получаем объект рабочего листа для форматирования
            worksheet = writer.sheets['Отчет по продажам']
            
            # Форматируем колонку Оборот как число с 2 знаками после запятой
            from openpyxl.styles import NamedStyle
            currency_style = NamedStyle(name='currency', number_format='#,##0.00')
            
            # Применяем стиль к колонке Оборот (обычно колонка E, если считать с A)
            for row in range(2, len(df) + 2):  # Начинаем с 2 строки (пропуская заголовок)
                worksheet[f'E{row}'].number_format = '#,##0.00'
                
            # Автоширина колонок
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\n📄 ОТЧЕТ СОХРАНЕН:")
        print(f"  Excel: {excel_file.absolute()}")
        
        return df
        
    def print_sales_report_summary(self, df):
        """Выводит краткую сводку отчета с продажами"""
        print(f"\n📈 СВОДКА ОТЧЕТА ПО ПРОДАЖАМ:")
        print("-" * 50)
        
        total_shops = len(df)
        total_receipts = int(df['Количество_чеков'].sum())
        total_revenue = float(df['Оборот'].sum())
        total_quantity = float(df['Количество_товаров'].sum())
        
        print(f"📊 Всего магазинов: {total_shops}")
        print(f"🧾 Всего чеков: {total_receipts:,}")
        print(f"💰 Общий оборот: {total_revenue:,.2f}")
        print(f"📦 Общее количество товаров: {total_quantity:,.1f}")
        
        if total_receipts > 0:
            avg_receipt = total_revenue / total_receipts
            print(f"🧾 Средний чек: {avg_receipt:.2f}")
        
        if total_shops > 0:
            avg_revenue_per_shop = total_revenue / total_shops
            print(f"🏪 Средний оборот на магазин: {avg_revenue_per_shop:,.2f}")
        
        print(f"\n🏆 ТОП-5 МАГАЗИНОВ ПО ОБОРОТУ:")
        top5_revenue = df.nlargest(5, 'Оборот')
        for i, (_, row) in enumerate(top5_revenue.iterrows(), 1):
            revenue = float(row['Оборот'])
            print(f"  {i}. {row['Название_магазина']} ({row['Город']}) - {revenue:,.2f}")
            
        print(f"\n🏆 ТОП-5 МАГАЗИНОВ ПО КОЛИЧЕСТВУ ЧЕКОВ:")
        top5_receipts = df.nlargest(5, 'Количество_чеков')
        for i, (_, row) in enumerate(top5_receipts.iterrows(), 1):
            receipts = int(row['Количество_чеков'])
            print(f"  {i}. {row['Название_магазина']} ({row['Город']}) - {receipts} чеков")
            
        print(f"\n🏙️  ПО ГОРОДАМ:")
        city_summary = df.groupby('Город').agg({
            'Количество_чеков': ['count', 'sum'],
            'Оборот': 'sum',
            'Количество_товаров': 'sum'
        }).round(2)
        
        for city in city_summary.index:
            shops_count = int(city_summary.loc[city, ('Количество_чеков', 'count')])
            receipts_count = int(city_summary.loc[city, ('Количество_чеков', 'sum')])
            revenue = float(city_summary.loc[city, ('Оборот', 'sum')])
            quantity = float(city_summary.loc[city, ('Количество_товаров', 'sum')])
            print(f"  {city}: {shops_count} магазинов, {receipts_count} чеков, оборот: {revenue:,.2f}, товаров: {quantity:,.1f}")
            
    def run_sales_report_generation(self):
        """Запускает генерацию отчета с показателями продаж"""
        print("=" * 70)
        print("🚀 ГЕНЕРАЦИЯ РАСШИРЕННОГО ОТЧЕТА ПО ПРОДАЖАМ")
        print("=" * 70)
        
        # Параметры отчета
        start_date = "2025-06-01"
        end_date = "2025-06-01"
        
        print(f"📅 Период: {start_date} - {end_date}")
        print(f"🏙️  Города: {', '.join(self.cities.values())}")
        print(f"📊 Показатели: Количество чеков, Оборот, Количество товаров")
        
        # Спрашиваем об очистке кеша
        if not self.ask_clear_cache():
            return False
            
        # Подключаемся к FTP
        ftp = self.connect_ftp()
        if not ftp:
            print("❌ Не удалось подключиться к FTP-серверу")
            return False
            
        try:
            # Загружаем справочники магазинов
            shops_data = self.download_shop_files(ftp)
            
            # Загружаем файлы чеков
            receipts_data = self.download_receipt_files(ftp, start_date, end_date)
            
            # Загружаем файлы товарных позиций
            cartitems_data = self.download_cartitem_files(ftp, start_date, end_date)
            
            # Закрываем соединение
            ftp.quit()
            print("\n✓ Соединение с FTP закрыто")
            
            # Консолидируем данные с показателями продаж
            report_data = self.consolidate_data_with_sales(shops_data, receipts_data, cartitems_data)
            
            # Сохраняем отчет
            df = self.save_excel_report(report_data, start_date, end_date)
            
            # Выводим сводку
            self.print_sales_report_summary(df)
            
            print(f"\n✅ ОТЧЕТ УСПЕШНО СГЕНЕРИРОВАН!")
            print(f"📁 Папка отчетов: {self.reports_dir.absolute()}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка в процессе генерации отчета: {e}")
            try:
                ftp.quit()
            except:
                pass
            return False

def main():
    """Главная функция программы"""
    print("FTP Analyzer v3.0 - Расширенный анализатор продаж")
    print("Автор: Система аналитики данных\n")
    
    try:
        analyzer = FTPAnalyzer()
        
        # Проверяем наличие необходимых библиотек
        try:
            import pandas as pd
            import openpyxl
        except ImportError as e:
            print("❌ Отсутствуют необходимые библиотеки!")
            print("Установите их командами:")
            print("pip install pandas openpyxl")
            return
        
        # Сразу запускаем генерацию отчета
        analyzer.run_sales_report_generation()
                
    except KeyboardInterrupt:
        print("\n\n⏹️  Программа прервана пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")

if __name__ == "__main__":
    main()