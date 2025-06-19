#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FTP Analyzer - Этап 2: Генерация отчета с кешированием данных
Автор: Система аналитики данных
Версия: 2.0
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
        
    def consolidate_data(self, shops_data, receipts_data):
        """Консолидирует данные и создает отчет"""
        print("\n📊 КОНСОЛИДАЦИЯ ДАННЫХ")
        print("-" * 30)
        
        report_data = []
        
        for city_code, city_name in self.cities.items():
            print(f"\n🏙️  Обрабатываем {city_name}...")
            
            if city_code not in shops_data:
                print(f"  ⚠️  Нет данных о магазинах для {city_name}")
                continue
                
            shops_df = shops_data[city_code]
            
            if city_code not in receipts_data or not receipts_data[city_code]:
                print(f"  ⚠️  Нет данных о чеках для {city_name}")
                # Добавляем магазины с нулевыми чеками
                for _, shop in shops_df.iterrows():
                    report_data.append({
                        'Город': city_name,
                        'ID_магазина': shop['id'],
                        'Название_магазина': shop['name'],
                        'Количество_чеков': 0
                    })
                continue
                
            # Объединяем все чеки по городу
            all_receipts = pd.concat(receipts_data[city_code], ignore_index=True)
            
            # Считаем количество чеков по каждому магазину
            receipt_counts = all_receipts.groupby('shop_id').size().reset_index(name='count')
            
            # Объединяем с данными о магазинах
            merged = shops_df.merge(receipt_counts, left_on='id', right_on='shop_id', how='left')
            merged['count'] = merged['count'].fillna(0).astype(int)
            
            # Добавляем в итоговый отчет
            for _, row in merged.iterrows():
                report_data.append({
                    'Город': city_name,
                    'ID_магазина': row['id'],
                    'Название_магазина': row['name'],
                    'Количество_чеков': row['count']
                })
                
            total_receipts = len(all_receipts)
            total_shops = len(shops_df)
            print(f"  ✓ Магазинов: {total_shops}, Чеков: {total_receipts}")
            
        return report_data
        
    def save_report(self, report_data, start_date, end_date):
        """Сохраняет отчет в CSV и Excel"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Создаем DataFrame
        df = pd.DataFrame(report_data)
        
        # Сортируем по городу и количеству чеков
        df = df.sort_values(['Город', 'Количество_чеков'], ascending=[True, False])
        
        # Сохраняем в CSV с разделителем |
        csv_file = self.reports_dir / f"report_{start_date}_{end_date}_{timestamp}.csv"
        df.to_csv(csv_file, sep='|', index=False, encoding='utf-8')
        
        # Сохраняем в Excel
        excel_file = self.reports_dir / f"report_{start_date}_{end_date}_{timestamp}.xlsx"
        df.to_excel(excel_file, index=False, engine='openpyxl')
        
        print(f"\n📄 ОТЧЕТ СОХРАНЕН:")
        print(f"  CSV: {csv_file.absolute()}")
        print(f"  Excel: {excel_file.absolute()}")
        
        return df
        
    def print_report_summary(self, df):
        """Выводит краткую сводку отчета"""
        print(f"\n📈 СВОДКА ОТЧЕТА:")
        print("-" * 40)
        
        total_shops = len(df)
        total_receipts = df['Количество_чеков'].sum()
        
        print(f"📊 Всего магазинов: {total_shops}")
        print(f"🧾 Всего чеков: {total_receipts}")
        print(f"📅 Средний чек на магазин: {total_receipts/total_shops:.1f}")
        
        print(f"\n🏆 ТОП-5 МАГАЗИНОВ ПО ЧЕКАМ:")
        top5 = df.nlargest(5, 'Количество_чеков')
        for i, (_, row) in enumerate(top5.iterrows(), 1):
            print(f"  {i}. {row['Название_магазина']} ({row['Город']}) - {row['Количество_чеков']} чеков")
            
        print(f"\n🏙️  ПО ГОРОДАМ:")
        city_summary = df.groupby('Город').agg({
            'Количество_чеков': ['count', 'sum']
        }).round(1)
        
        for city in city_summary.index:
            shops_count = city_summary.loc[city, ('Количество_чеков', 'count')]
            receipts_count = city_summary.loc[city, ('Количество_чеков', 'sum')]
            print(f"  {city}: {shops_count} магазинов, {receipts_count} чеков")
            
    def run_report_generation(self):
        """Запускает генерацию отчета"""
        print("=" * 60)
        print("🚀 ЭТАП 2: ГЕНЕРАЦИЯ ОТЧЕТА")
        print("=" * 60)
        
        # Параметры отчета
        start_date = "2025-06-10"
        end_date = "2025-06-17"
        
        print(f"📅 Период: {start_date} - {end_date}")
        print(f"🏙️  Города: {', '.join(self.cities.values())}")
        
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
            
            # Закрываем соединение
            ftp.quit()
            print("\n✓ Соединение с FTP закрыто")
            
            # Консолидируем данные
            report_data = self.consolidate_data(shops_data, receipts_data)
            
            # Сохраняем отчет
            df = self.save_report(report_data, start_date, end_date)
            
            # Выводим сводку
            self.print_report_summary(df)
            
            print(f"\n✅ ЭТАП 2 ЗАВЕРШЕН УСПЕШНО!")
            print(f"📁 Папка отчетов: {self.reports_dir.absolute()}")
            print("\n💡 Для перехода к следующему этапу напишите 'ОК'")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка в процессе генерации отчета: {e}")
            try:
                ftp.quit()
            except:
                pass
            return False
            
    def scan_directory(self, ftp, path="/"):
        """Сканирует директорию на FTP-сервере"""
        items = []
        try:
            print(f"🔍 Сканируем папку: {path}")
            
            ftp_items = []
            ftp.retrlines('LIST ' + path, ftp_items.append)
            
            for item in ftp_items:
                parts = item.split()
                if len(parts) >= 9:
                    permissions = parts[0]
                    name = ' '.join(parts[8:])
                    
                    if name in ['.', '..']:
                        continue
                        
                    is_directory = permissions.startswith('d')
                    
                    item_info = {
                        'name': name,
                        'path': f"{path.rstrip('/')}/{name}",
                        'type': 'directory' if is_directory else 'file',
                        'size': parts[4] if not is_directory else 0,
                        'permissions': permissions,
                        'modified': ' '.join(parts[5:8])
                    }
                    
                    items.append(item_info)
                    
            print(f"  📁 Найдено элементов: {len(items)}")
            return items
            
        except Exception as e:
            print(f"✗ Ошибка сканирования папки {path}: {e}")
            return []
            
    def scan_ftp_structure(self, ftp, start_path="/www", max_depth=3, current_depth=0):
        """Рекурсивно сканирует структуру FTP начиная с указанной папки"""
        structure = {
            'path': start_path,
            'type': 'directory',
            'children': [],
            'scanned_at': datetime.now().isoformat()
        }
        
        if current_depth >= max_depth:
            print(f"⚠️  Достигнута максимальная глубина сканирования ({max_depth}) для {start_path}")
            return structure
            
        items = self.scan_directory(ftp, start_path)
        
        for item in items:
            if item['type'] == 'file' and item['name'].lower().endswith('.csv'):
                structure['children'].append({
                    'name': item['name'],
                    'path': item['path'],
                    'type': 'file',
                    'size': item['size'],
                    'format': 'csv'
                })
                print(f"  📄 CSV файл: {item['name']} ({item['size']} байт)")
                
            elif item['type'] == 'directory':
                print(f"  📁 Переходим в папку: {item['name']}")
                subdirectory = self.scan_ftp_structure(
                    ftp, 
                    item['path'], 
                    max_depth, 
                    current_depth + 1
                )
                structure['children'].append(subdirectory)
                
        return structure
        
    def save_structure(self, structure):
        """Сохраняет структуру FTP в JSON файл"""
        try:
            with open(self.structure_file, 'w', encoding='utf-8') as f:
                json.dump(structure, f, ensure_ascii=False, indent=2)
            print(f"✓ Структура сохранена в файл: {self.structure_file.absolute()}")
            return True
        except Exception as e:
            print(f"✗ Ошибка сохранения структуры: {e}")
            return False
            
    def print_structure_summary(self, structure, level=0):
        """Выводит краткую сводку структуры"""
        indent = "  " * level
        
        if structure['type'] == 'directory':
            print(f"{indent}📁 {structure['path']}")
            csv_files = [child for child in structure.get('children', []) if child.get('format') == 'csv']
            directories = [child for child in structure.get('children', []) if child['type'] == 'directory']
            
            if csv_files:
                print(f"{indent}  📄 CSV файлов: {len(csv_files)}")
                
            for child in structure.get('children', []):
                if child['type'] == 'directory':
                    self.print_structure_summary(child, level + 1)
                    
    def run_structure_scan(self):
        """Запускает сканирование структуры FTP"""
        print("=" * 60)
        print("🚀 ЭТАП 1: СКАНИРОВАНИЕ СТРУКТУРЫ FTP-СЕРВЕРА")
        print("=" * 60)
        
        ftp = self.connect_ftp()
        if not ftp:
            print("❌ Не удалось подключиться к FTP-серверу")
            return False
            
        try:
            print("\n🔍 Начинаем сканирование структуры...")
            structure = self.scan_ftp_structure(ftp, "/www", max_depth=3)
            
            ftp.quit()
            print("✓ Соединение с FTP закрыто")
            
            if self.save_structure(structure):
                print("\n📋 КРАТКАЯ СВОДКА СТРУКТУРЫ:")
                print("-" * 40)
                self.print_structure_summary(structure)
                
                print(f"\n✅ ЭТАП 1 ЗАВЕРШЕН УСПЕШНО!")
                print(f"📁 Рабочая папка: {self.base_dir.absolute()}")
                print(f"📄 Файл структуры: {self.structure_file.absolute()}")
                return True
            else:
                print("❌ Ошибка сохранения структуры")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка в процессе сканирования: {e}")
            try:
                ftp.quit()
            except:
                pass
            return False

def main():
    """Главная функция программы"""
    print("FTP Analyzer v2.0 - Анализатор данных FTP-сервера")
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
            input("Нажмите Enter для завершения...")
            return
        
        print("🎯 Выберите действие:")
        print("1. Сканировать структуру FTP (Этап 1)")
        print("2. Сгенерировать отчет (Этап 2)")
        
        while True:
            choice = input("\nВведите номер (1 или 2): ").strip()
            if choice == "1":
                success = analyzer.run_structure_scan()
                break
            elif choice == "2":
                success = analyzer.run_report_generation()
                break
            else:
                print("❌ Введите '1' или '2'")
                
        if success:
            input("\n⏸️  Нажмите Enter для завершения...")
        else:
            input("\n❌ Программа завершена с ошибками. Нажмите Enter...")
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Программа прервана пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        input("Нажмите Enter для завершения...")

if __name__ == "__main__":
    main()