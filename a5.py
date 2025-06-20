#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FTP Analyzer - Этап 6: Детализированный отчет по списаниям
Автор: Система аналитики данных
Версия: 6.0
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
            
            if 'lossproduct' in file_name:
                # Для файлов lossproduct преобразуем qty и total_price в числа
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
        
    def download_losstype_file(self, ftp):
        """Скачивает справочник типов списаний"""
        print("\n📋 ЗАГРУЗКА СПРАВОЧНИКА ТИПОВ СПИСАНИЙ")
        print("-" * 50)
        
        remote_path = "/www/losstype.csv"
        local_path = self.cache_dir / "losstype.csv"
        
        if self.download_file(ftp, remote_path, local_path):
            df = self.read_csv_with_pipe(local_path)
            if df is not None:
                print(f"  📊 Типов списаний: {len(df)}")
                return df
        
        return None
        
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
        
    def download_loss_data(self, ftp, start_date, end_date):
        """Скачивает данные о списаниях и продуктах списаний за указанный период"""
        print(f"\n📉 ЗАГРУЗКА ДАННЫХ О СПИСАНИЯХ ({start_date} - {end_date})")
        print("-" * 70)
        
        dates = self.generate_date_range(start_date, end_date)
        loss_data = {}
        lossproduct_data = {}
        
        for city_code, city_name in self.cities.items():
            print(f"\n🏙️  {city_name} ({city_code}):")
            loss_data[city_code] = []
            lossproduct_data[city_code] = []
            
            for date in dates:
                # Скачиваем документы списаний
                loss_remote = f"/www/loss/loss_{city_code}_{date}.csv"
                loss_local = self.cache_dir / f"loss_{city_code}_{date}.csv"
                
                # Скачиваем продукты списаний
                lossproduct_remote = f"/www/lossproduct/lossproduct_{city_code}_{date}.csv"
                lossproduct_local = self.cache_dir / f"lossproduct_{city_code}_{date}.csv"
                
                loss_loaded = False
                lossproduct_loaded = False
                
                # Загружаем документы списаний
                if self.download_file(ftp, loss_remote, loss_local):
                    df = self.read_csv_with_pipe(loss_local)
                    if df is not None:
                        df['date'] = date
                        loss_data[city_code].append(df)
                        loss_loaded = True
                        
                # Загружаем продукты списаний
                if self.download_file(ftp, lossproduct_remote, lossproduct_local):
                    df = self.read_csv_with_pipe(lossproduct_local)
                    if df is not None:
                        df['date'] = date
                        lossproduct_data[city_code].append(df)
                        lossproduct_loaded = True
                        
                if loss_loaded and lossproduct_loaded:
                    loss_df = loss_data[city_code][-1]
                    lossproduct_df = lossproduct_data[city_code][-1]
                    total_qty = float(lossproduct_df['qty'].sum()) if 'qty' in lossproduct_df.columns else 0.0
                    total_price = float(lossproduct_df['total_price'].sum()) if 'total_price' in lossproduct_df.columns else 0.0
                    print(f"    📅 {date}: {len(loss_df)} документов, {len(lossproduct_df)} позиций, товаров: {total_qty:.1f}, сумма: {total_price:.2f}")
                elif loss_loaded:
                    loss_df = loss_data[city_code][-1]
                    print(f"    📅 {date}: {len(loss_df)} документов списаний")
                    
        return loss_data, lossproduct_data
        
    def consolidate_detailed_loss_data(self, shops_data, losstype_data, loss_data, lossproduct_data):
        """Консолидирует детализированные данные по списаниям и создает отчет"""
        print("\n📊 КОНСОЛИДАЦИЯ ДЕТАЛИЗИРОВАННЫХ ДАННЫХ ПО СПИСАНИЯМ")
        print("-" * 60)
        
        if losstype_data is None:
            print("❌ Отсутствует справочник типов списаний")
            return []
        
        report_data = []
        
        for city_code, city_name in self.cities.items():
            print(f"\n🏙️  Обрабатываем {city_name}...")
            
            if city_code not in shops_data:
                print(f"  ⚠️  Нет данных о магазинах для {city_name}")
                continue
                
            shops_df = shops_data[city_code]
            
            # Инициализируем данные для всех магазинов и типов списаний
            for _, shop in shops_df.iterrows():
                for _, losstype in losstype_data.iterrows():
                    report_data.append({
                        'Город': city_name,
                        'ID_магазина': shop['id'],
                        'Название_магазина': shop['name'],
                        'ID_типа_списания': losstype['id'],
                        'Тип_списания': losstype['name'],
                        'Количество_документов': 0,
                        'Сумма_списаний': 0.0,
                        'Количество_товаров': 0.0
                    })
            
            if city_code not in loss_data or not loss_data[city_code]:
                print(f"  ⚠️  Нет данных о списаниях для {city_name}")
                continue
                
            # Объединяем все документы списаний по городу
            all_losses = pd.concat(loss_data[city_code], ignore_index=True)
            
            # Проверяем связь между списаниями и магазинами
            valid_shop_ids = set(shops_df['id'].astype(str))
            valid_losses = all_losses[all_losses['shop_id'].astype(str).isin(valid_shop_ids)]
            
            if len(valid_losses) < len(all_losses):
                invalid_count = len(all_losses) - len(valid_losses)
                print(f"  ⚠️  Исключено {invalid_count} документов с неверными shop_id")
            
            if len(valid_losses) == 0:
                print(f"  ⚠️  Нет корректных данных о списаниях для {city_name}")
                continue
            
            # Считаем количество документов по магазинам и типам
            loss_counts = valid_losses.groupby(['shop_id', 'type_id']).size().reset_index(name='document_count')
            
            # Обрабатываем продукты списаний если есть данные
            loss_with_products = None
            if city_code in lossproduct_data and lossproduct_data[city_code]:
                print(f"  🛒 Обрабатываем продукты списаний...")
                
                # Объединяем все продукты списаний по городу
                all_lossproducts = pd.concat(lossproduct_data[city_code], ignore_index=True)
                
                # Связываем продукты с документами списаний
                lossproducts_with_docs = all_lossproducts.merge(
                    valid_losses[['id', 'shop_id', 'type_id']], 
                    left_on='document_id', 
                    right_on='id', 
                    how='inner'
                )
                
                if not lossproducts_with_docs.empty:
                    # Группируем по магазинам и типам списаний
                    loss_with_products = lossproducts_with_docs.groupby(['shop_id', 'type_id']).agg({
                        'total_price': 'sum',
                        'qty': 'sum'
                    }).reset_index()
                    
                    # Убеждаемся что данные в правильном формате
                    loss_with_products['total_price'] = pd.to_numeric(loss_with_products['total_price'], errors='coerce').fillna(0.0)
                    loss_with_products['qty'] = pd.to_numeric(loss_with_products['qty'], errors='coerce').fillna(0.0)
                    
                    print(f"    ✓ Обработано продуктов списаний: {len(all_lossproducts)}")
                    print(f"    ✓ Связано с документами: {len(lossproducts_with_docs)}")
                else:
                    print(f"    ⚠️  Не удалось связать продукты с документами списаний")
            else:
                print(f"  ⚠️  Нет данных о продуктах списаний для {city_name}")
            
            # Обновляем данные отчета
            for i, row in enumerate(report_data):
                if row['Город'] == city_name:
                    shop_id = str(row['ID_магазина'])
                    type_id = str(row['ID_типа_списания'])
                    
                    # Обновляем количество документов
                    doc_match = loss_counts[
                        (loss_counts['shop_id'].astype(str) == shop_id) & 
                        (loss_counts['type_id'].astype(str) == type_id)
                    ]
                    if not doc_match.empty:
                        report_data[i]['Количество_документов'] = int(doc_match.iloc[0]['document_count'])
                    
                    # Обновляем сумму и количество товаров
                    if loss_with_products is not None:
                        product_match = loss_with_products[
                            (loss_with_products['shop_id'].astype(str) == shop_id) & 
                            (loss_with_products['type_id'].astype(str) == type_id)
                        ]
                        if not product_match.empty:
                            report_data[i]['Сумма_списаний'] = float(product_match.iloc[0]['total_price'])
                            report_data[i]['Количество_товаров'] = float(product_match.iloc[0]['qty'])
            
            total_documents = len(all_losses)
            valid_documents = len(valid_losses)
            total_amount = 0.0
            total_quantity = 0.0
            
            if loss_with_products is not None:
                total_amount = float(loss_with_products['total_price'].sum())
                total_quantity = float(loss_with_products['qty'].sum())
            
            print(f"  ✓ Всего документов: {total_documents}")
            print(f"  ✓ Корректных документов: {valid_documents}")
            print(f"  ✓ Общая сумма списаний: {total_amount:.2f}")
            print(f"  ✓ Общее количество товаров: {total_quantity:.1f}")
            
        return report_data
        
    def save_detailed_loss_excel_report(self, report_data, start_date, end_date):
        """Сохраняет детализированный отчет по списаниям в Excel"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Создаем DataFrame
        df = pd.DataFrame(report_data)
        
        # Фильтруем записи с нулевыми значениями для краткости
        df_filtered = df[
            (df['Количество_документов'] > 0) | 
            (df['Сумма_списаний'] > 0) | 
            (df['Количество_товаров'] > 0)
        ].copy()
        
        # Если нет активных записей, показываем все
        if df_filtered.empty:
            df_filtered = df.copy()
        
        # Сортируем по городу, магазину и сумме списаний
        df_filtered = df_filtered.sort_values(['Город', 'Название_магазина', 'Сумма_списаний'], ascending=[True, True, False])
        
        # Форматируем числовые поля
        df_filtered['Количество_документов'] = df_filtered['Количество_документов'].astype(int)
        df_filtered['Сумма_списаний'] = pd.to_numeric(df_filtered['Сумма_списаний'], errors='coerce').fillna(0.0).round(2)
        df_filtered['Количество_товаров'] = pd.to_numeric(df_filtered['Количество_товаров'], errors='coerce').fillna(0.0).round(1)
        
        # Сохраняем в Excel
        excel_file = self.reports_dir / f"detailed_loss_report_{start_date}_{end_date}_{timestamp}.xlsx"
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            df_filtered.to_excel(writer, sheet_name='Детализированный отчет', index=False)
            
            # Получаем объект рабочего листа для форматирования
            worksheet = writer.sheets['Детализированный отчет']
            
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
                adjusted_width = min(max_length + 2, 40)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\n📄 ДЕТАЛИЗИРОВАННЫЙ ОТЧЕТ ПО СПИСАНИЯМ СОХРАНЕН:")
        print(f"  Excel: {excel_file.absolute()}")
        print(f"  📊 Записей в отчете: {len(df_filtered)} (из {len(df)} всего)")
        
        return df_filtered
        
    def print_detailed_loss_report_summary(self, df):
        """Выводит краткую сводку детализированного отчета по списаниям"""
        print(f"\n📈 СВОДКА ДЕТАЛИЗИРОВАННОГО ОТЧЕТА ПО СПИСАНИЯМ:")
        print("-" * 60)
        
        total_documents = int(df['Количество_документов'].sum())
        total_amount = float(df['Сумма_списаний'].sum())
        total_quantity = float(df['Количество_товаров'].sum())
        active_records = len(df[df['Количество_документов'] > 0])
        
        print(f"📊 Активных записей в отчете: {active_records}")
        print(f"📉 Общее количество документов списаний: {total_documents:,}")
        print(f"💰 Общая сумма списаний: {total_amount:,.2f}")
        print(f"📦 Общее количество товаров: {total_quantity:,.1f}")
        
        if total_documents > 0:
            avg_amount_per_doc = total_amount / total_documents
            avg_quantity_per_doc = total_quantity / total_documents
            print(f"📈 Средняя сумма на документ: {avg_amount_per_doc:.2f}")
            print(f"📈 Среднее количество товаров на документ: {avg_quantity_per_doc:.1f}")
        
        # ТОП магазинов по сумме списаний
        print(f"\n🏆 ТОП-5 МАГАЗИНОВ ПО СУММЕ СПИСАНИЙ:")
        shop_summary = df.groupby(['Город', 'Название_магазина']).agg({
            'Количество_документов': 'sum',
            'Сумма_списаний': 'sum',
            'Количество_товаров': 'sum'
        }).reset_index().sort_values('Сумма_списаний', ascending=False)
        
        for i, (_, row) in enumerate(shop_summary.head(5).iterrows(), 1):
            if row['Сумма_списаний'] > 0:
                print(f"  {i}. {row['Название_магазина']} ({row['Город']}) - {float(row['Сумма_списаний']):,.2f}, {int(row['Количество_документов'])} док.")
        
        # ТОП типов списаний
        print(f"\n🏆 ТОП-5 ТИПОВ СПИСАНИЙ ПО СУММЕ:")
        type_summary = df.groupby('Тип_списания').agg({
            'Количество_документов': 'sum',
            'Сумма_списаний': 'sum',
            'Количество_товаров': 'sum'
        }).reset_index().sort_values('Сумма_списаний', ascending=False)
        
        for i, (_, row) in enumerate(type_summary.head(5).iterrows(), 1):
            if row['Сумма_списаний'] > 0:
                print(f"  {i}. {row['Тип_списания']} - {float(row['Сумма_списаний']):,.2f}, {int(row['Количество_документов'])} док.")
        
        # По городам
        print(f"\n🏙️  ПО ГОРОДАМ:")
        city_summary = df.groupby('Город').agg({
            'Количество_документов': 'sum',
            'Сумма_списаний': 'sum',
            'Количество_товаров': 'sum'
        }).round(2)
        
        for city in city_summary.index:
            docs_count = int(city_summary.loc[city, 'Количество_документов'])
            amount = float(city_summary.loc[city, 'Сумма_списаний'])
            quantity = float(city_summary.loc[city, 'Количество_товаров'])
            print(f"  {city}: {docs_count} документов, сумма: {amount:,.2f}, товаров: {quantity:,.1f}")
                
    def run_detailed_loss_report_generation(self):
        """Запускает генерацию детализированного отчета по списаниям"""
        print("=" * 80)
        print("🚀 ГЕНЕРАЦИЯ ДЕТАЛИЗИРОВАННОГО ОТЧЕТА ПО СПИСАНИЯМ")
        print("=" * 80)
        
        # Параметры отчета
        start_date = "2025-05-01"
        end_date = "2025-06-05"
        
        print(f"📅 Период: {start_date} - {end_date}")
        print(f"🏙️  Города: {', '.join(self.cities.values())}")
        print(f"📊 Показатели: Город - Магазин - Тип списания - Количество документов - Сумма - Количество товаров")
        
        # Спрашиваем об очистке кеша
        if not self.ask_clear_cache():
            return False
            
        # Подключаемся к FTP
        ftp = self.connect_ftp()
        if not ftp:
            print("❌ Не удалось подключиться к FTP-серверу")
            return False
            
        try:
            # Загружаем справочник типов списаний
            losstype_data = self.download_losstype_file(ftp)
            
            # Загружаем справочники магазинов
            shops_data = self.download_shop_files(ftp)
            
            # Загружаем данные о списаниях и продуктах
            loss_data, lossproduct_data = self.download_loss_data(ftp, start_date, end_date)
            
            # Закрываем соединение
            ftp.quit()
            print("\n✓ Соединение с FTP закрыто")
            
            # Консолидируем детализированные данные по списаниям
            report_data = self.consolidate_detailed_loss_data(shops_data, losstype_data, loss_data, lossproduct_data)
            
            if not report_data:
                print("❌ Нет данных для создания отчета")
                return False
            
            # Сохраняем отчет
            df = self.save_detailed_loss_excel_report(report_data, start_date, end_date)
            
            # Выводим сводку
            self.print_detailed_loss_report_summary(df)
            
            print(f"\n✅ ДЕТАЛИЗИРОВАННЫЙ ОТЧЕТ ПО СПИСАНИЯМ УСПЕШНО СГЕНЕРИРОВАН!")
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
    print("FTP Analyzer v6.0 - Детализированный анализатор списаний")
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
        
        # Сразу запускаем генерацию детализированного отчета по списаниям
        analyzer.run_detailed_loss_report_generation()
                
    except KeyboardInterrupt:
        print("\n\n⏹️  Программа прервана пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")

if __name__ == "__main__":
    main()