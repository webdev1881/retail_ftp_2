#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FTP Analyzer - Этап 7: Веб-приложение с интерфейсом
Автор: Система аналитики данных
Версия: 7.0
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
import webbrowser
import threading
import time

# Импорт Flask
try:
    from flask import Flask, render_template_string, request, jsonify, send_file
except ImportError:
    print("❌ Библиотека Flask не установлена!")
    print("Установите командой: pip install flask")
    sys.exit(1)

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
            
    def connect_ftp(self):
        """Подключается к FTP-серверу"""
        try:
            print(f"🔄 Подключаемся к FTP-серверу {self.ftp_host}:{self.ftp_port}...")
            ftp = ftplib.FTP()
            ftp.connect(self.ftp_host, self.ftp_port)
            ftp.login(self.ftp_user, self.ftp_pass)
            print("✓ Подключение к FTP установлено")
            return ftp
        except Exception as e:
            print(f"✗ Ошибка подключения к FTP: {e}")
            return None
            
    def download_file(self, ftp, remote_path, local_path):
        """Скачивает файл с FTP сервера"""
        try:
            if local_path.exists():
                return True
                
            print(f"⬇️  Скачиваем: {remote_path}")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {remote_path}', f.write)
            
            print(f"✓ Скачан: {local_path.name}")
            return True
            
        except ftplib.error_perm as e:
            if "550" in str(e):
                print(f"⚠️  Файл не найден: {remote_path}")
            return False
        except Exception as e:
            print(f"✗ Ошибка скачивания {remote_path}: {e}")
            return False
            
    def read_csv_with_pipe(self, file_path):
        """Читает CSV файл с разделителем |"""
        try:
            df = pd.read_csv(file_path, sep='|', encoding='utf-8')
            
            file_name = file_path.name.lower()
            
            if 'cartitem' in file_name or 'lossproduct' in file_name:
                if 'qty' in df.columns:
                    df['qty'] = df['qty'].astype(str).str.replace(',', '.', regex=False)
                    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
                if 'total_price' in df.columns:
                    df['total_price'] = df['total_price'].astype(str).str.replace(',', '.', regex=False)
                    df['total_price'] = pd.to_numeric(df['total_price'], errors='coerce').fillna(0.0)
            
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()
            
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
        
    def generate_sales_report_api(self, start_date, end_date, selected_cities):
        """API версия генерации отчета по продажам"""
        ftp = self.connect_ftp()
        if not ftp:
            return {"error": "Не удалось подключиться к FTP"}
            
        try:
            # Загружаем справочники магазинов
            shops_data = {}
            for city_code, city_name in self.cities.items():
                if city_name not in selected_cities:
                    continue
                    
                remote_path = f"/www/shop_{city_code}.csv"
                local_path = self.cache_dir / f"shop_{city_code}.csv"
                
                if self.download_file(ftp, remote_path, local_path):
                    df = self.read_csv_with_pipe(local_path)
                    if df is not None:
                        shops_data[city_code] = df
            
            # Загружаем данные о продажах
            dates = self.generate_date_range(start_date, end_date)
            sales_data = []
            
            for city_code, city_name in self.cities.items():
                if city_name not in selected_cities:
                    continue
                    
                if city_code not in shops_data:
                    continue
                    
                shops_df = shops_data[city_code]
                
                for date in dates:
                    # Чеки
                    receipt_remote = f"/www/receipt/receipt_{city_code}_{date}.csv"
                    receipt_local = self.cache_dir / f"receipt_{city_code}_{date}.csv"
                    
                    # Товары
                    cartitem_remote = f"/www/cartitem/cartitem_{city_code}_{date}.csv"
                    cartitem_local = self.cache_dir / f"cartitem_{city_code}_{date}.csv"
                    
                    receipts_df = None
                    cartitems_df = None
                    
                    if self.download_file(ftp, receipt_remote, receipt_local):
                        receipts_df = self.read_csv_with_pipe(receipt_local)
                        
                    if self.download_file(ftp, cartitem_remote, cartitem_local):
                        cartitems_df = self.read_csv_with_pipe(cartitem_local)
                    
                    if receipts_df is not None and cartitems_df is not None:
                        # Связываем товары с чеками
                        cartitems_with_shops = cartitems_df.merge(
                            receipts_df[['id', 'shop_id']], 
                            left_on='receipt_id', 
                            right_on='id', 
                            how='inner'
                        )
                        
                        if not cartitems_with_shops.empty:
                            # Группируем по магазинам
                            shop_sales = cartitems_with_shops.groupby('shop_id').agg({
                                'total_price': 'sum',
                                'qty': 'sum'
                            }).reset_index()
                            
                            # Добавляем информацию о магазинах
                            shop_sales = shop_sales.merge(shops_df, left_on='shop_id', right_on='id', how='left')
                            
                            for _, row in shop_sales.iterrows():
                                sales_data.append({
                                    'город': city_name,
                                    'магазин': row['name'] if pd.notna(row['name']) else f"Магазин {row['shop_id']}",
                                    'тип': 'Продажа',
                                    'дата': date,
                                    'количество': float(row['qty']) if pd.notna(row['qty']) else 0,
                                    'сумма': float(row['total_price']) if pd.notna(row['total_price']) else 0
                                })
            
            ftp.quit()
            return {"data": sales_data}
            
        except Exception as e:
            try:
                ftp.quit()
            except:
                pass
            return {"error": str(e)}
    
    def generate_losses_report_api(self, start_date, end_date, selected_cities):
        """API версия генерации отчета по списаниям"""
        ftp = self.connect_ftp()
        if not ftp:
            return {"error": "Не удалось подключиться к FTP"}
            
        try:
            # Загружаем справочники
            losstype_remote = "/www/losstype.csv"
            losstype_local = self.cache_dir / "losstype.csv"
            losstype_df = None
            
            if self.download_file(ftp, losstype_remote, losstype_local):
                losstype_df = self.read_csv_with_pipe(losstype_local)
            
            if losstype_df is None:
                return {"error": "Не удалось загрузить справочник типов списаний"}
            
            shops_data = {}
            for city_code, city_name in self.cities.items():
                if city_name not in selected_cities:
                    continue
                    
                remote_path = f"/www/shop_{city_code}.csv"
                local_path = self.cache_dir / f"shop_{city_code}.csv"
                
                if self.download_file(ftp, remote_path, local_path):
                    df = self.read_csv_with_pipe(local_path)
                    if df is not None:
                        shops_data[city_code] = df
            
            # Загружаем данные о списаниях
            dates = self.generate_date_range(start_date, end_date)
            losses_data = []
            
            for city_code, city_name in self.cities.items():
                if city_name not in selected_cities:
                    continue
                    
                if city_code not in shops_data:
                    continue
                    
                shops_df = shops_data[city_code]
                
                for date in dates:
                    # Документы списаний
                    loss_remote = f"/www/loss/loss_{city_code}_{date}.csv"
                    loss_local = self.cache_dir / f"loss_{city_code}_{date}.csv"
                    
                    # Продукты списаний
                    lossproduct_remote = f"/www/lossproduct/lossproduct_{city_code}_{date}.csv"
                    lossproduct_local = self.cache_dir / f"lossproduct_{city_code}_{date}.csv"
                    
                    loss_df = None
                    lossproduct_df = None
                    
                    if self.download_file(ftp, loss_remote, loss_local):
                        loss_df = self.read_csv_with_pipe(loss_local)
                        
                    if self.download_file(ftp, lossproduct_remote, lossproduct_local):
                        lossproduct_df = self.read_csv_with_pipe(lossproduct_local)
                    
                    if loss_df is not None and lossproduct_df is not None:
                        # Связываем продукты с документами
                        lossproducts_with_docs = lossproduct_df.merge(
                            loss_df[['id', 'shop_id', 'type_id']], 
                            left_on='document_id', 
                            right_on='id', 
                            how='inner'
                        )
                        
                        if not lossproducts_with_docs.empty:
                            # Группируем по магазинам и типам
                            loss_summary = lossproducts_with_docs.groupby(['shop_id', 'type_id']).agg({
                                'total_price': 'sum',
                                'qty': 'sum'
                            }).reset_index()
                            
                            # Добавляем информацию о магазинах и типах
                            loss_summary = loss_summary.merge(shops_df, left_on='shop_id', right_on='id', how='left')
                            loss_summary = loss_summary.merge(losstype_df, left_on='type_id', right_on='id', how='left', suffixes=('_shop', '_type'))
                            
                            for _, row in loss_summary.iterrows():
                                losses_data.append({
                                    'город': city_name,
                                    'магазин': row['name_shop'] if pd.notna(row['name_shop']) else f"Магазин {row['shop_id']}",
                                    'тип': row['name_type'] if pd.notna(row['name_type']) else f"Тип {row['type_id']}",
                                    'дата': date,
                                    'количество': float(row['qty']) if pd.notna(row['qty']) else 0,
                                    'сумма': float(row['total_price']) if pd.notna(row['total_price']) else 0
                                })
            
            ftp.quit()
            return {"data": losses_data}
            
        except Exception as e:
            try:
                ftp.quit()
            except:
                pass
            return {"error": str(e)}

# Создаем Flask приложение
app = Flask(__name__)
analyzer = FTPAnalyzer()

# HTML шаблон с интерфейсом
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FTP Analyzer - Веб-интерфейс</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }

        .header {
            background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }

        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            font-weight: 700;
        }

        .header p {
            font-size: 1.1rem;
            opacity: 0.9;
        }

        .main-content {
            display: grid;
            grid-template-columns: 350px 1fr;
            min-height: 600px;
        }

        .sidebar {
            background: #f8fafc;
            padding: 30px;
            border-right: 1px solid #e2e8f0;
        }

        .content-area {
            padding: 30px;
        }

        .section {
            background: white;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid #e2e8f0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }

        .section-title {
            font-size: 1.2rem;
            font-weight: 600;
            color: #1e293b;
            margin-bottom: 15px;
            display: flex;
            align-items: center;
        }

        .section-title::before {
            content: '';
            width: 4px;
            height: 20px;
            background: #4f46e5;
            margin-right: 10px;
            border-radius: 2px;
        }

        .form-group {
            margin-bottom: 15px;
        }

        label {
            display: block;
            font-weight: 500;
            color: #374151;
            margin-bottom: 5px;
        }

        select, input[type="date"] {
            width: 100%;
            padding: 10px 12px;
            border: 2px solid #e5e7eb;
            border-radius: 8px;
            font-size: 14px;
            transition: all 0.2s;
        }

        select:focus, input:focus {
            outline: none;
            border-color: #4f46e5;
            box-shadow: 0 0 0 3px rgba(79, 70, 229, 0.1);
        }

        .checkbox-group {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            margin-top: 10px;
        }

        .checkbox-item {
            display: flex;
            align-items: center;
            padding: 8px;
            border-radius: 6px;
            cursor: pointer;
            transition: background-color 0.2s;
        }

        .checkbox-item:hover {
            background-color: #f1f5f9;
        }

        .checkbox-item input[type="checkbox"] {
            margin-right: 8px;
            width: auto;
        }

        .btn {
            background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
            width: 100%;
            margin-top: 10px;
        }

        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(79, 70, 229, 0.3);
        }

        .btn:disabled {
            background: #94a3b8;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }

        .btn-secondary {
            background: linear-gradient(135deg, #64748b 0%, #475569 100%);
        }

        .results-section {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }

        .results-title {
            font-size: 1.5rem;
            font-weight: 600;
            color: #1e293b;
            margin-bottom: 20px;
        }

        .stats-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 25px;
        }

        .stat-card {
            background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            border: 1px solid #bae6fd;
        }

        .stat-value {
            font-size: 2rem;
            font-weight: 700;
            color: #0369a1;
            margin-bottom: 5px;
        }

        .stat-label {
            color: #64748b;
            font-size: 0.9rem;
        }

        .table-container {
            overflow-x: auto;
            border-radius: 8px;
            border: 1px solid #e2e8f0;
            max-height: 500px;
            overflow-y: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
        }

        th {
            background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
            color: white;
            padding: 15px 12px;
            text-align: left;
            font-weight: 600;
            font-size: 0.9rem;
            position: sticky;
            top: 0;
            z-index: 10;
        }

        td {
            padding: 12px;
            border-bottom: 1px solid #f1f5f9;
            color: #374151;
        }

        tr:hover {
            background-color: #f8fafc;
        }

        tr:nth-child(even) {
            background-color: #fafafa;
        }

        .no-data {
            text-align: center;
            padding: 60px 20px;
            color: #64748b;
            font-size: 1.1rem;
        }

        .no-data::before {
            content: '📊';
            font-size: 3rem;
            display: block;
            margin-bottom: 15px;
        }

        .loading {
            text-align: center;
            padding: 40px;
            color: #64748b;
        }

        .loading::before {
            content: '⚡';
            font-size: 2rem;
            display: block;
            margin-bottom: 10px;
            animation: pulse 1.5s infinite;
        }

        .error {
            background: #fef2f2;
            color: #dc2626;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #fecaca;
            margin-bottom: 20px;
        }

        .error::before {
            content: '❌ ';
            font-weight: bold;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        @media (max-width: 768px) {
            .main-content {
                grid-template-columns: 1fr;
            }
            
            .sidebar {
                border-right: none;
                border-bottom: 1px solid #e2e8f0;
            }
            
            .checkbox-group {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 FTP Analyzer</h1>
            <p>Веб-интерфейс для анализа данных продаж и списаний</p>
        </div>
        
        <div class="main-content">
            <div class="sidebar">
                <div class="section">
                    <div class="section-title">📋 Тип отчета</div>
                    <div class="form-group">
                        <label for="reportType">Выберите отчет:</label>
                        <select id="reportType">
                            <option value="sales">Продажи</option>
                            <option value="losses">Списания</option>
                        </select>
                    </div>
                </div>

                <div class="section">
                    <div class="section-title">📅 Период</div>
                    <div class="form-group">
                        <label for="startDate">Дата начала:</label>
                        <input type="date" id="startDate" value="2025-06-01">
                    </div>
                    <div class="form-group">
                        <label for="endDate">Дата окончания:</label>
                        <input type="date" id="endDate" value="2025-06-03">
                    </div>
                </div>

                <div class="section">
                    <div class="section-title">🏙️ Города</div>
                    <div class="checkbox-group">
                        <div class="checkbox-item">
                            <input type="checkbox" id="city-khar" value="Харьков" checked>
                            <label for="city-khar">Харьков</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="city-kiev" value="Киев" checked>
                            <label for="city-kiev">Киев</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="city-dnepr" value="Днепр" checked>
                            <label for="city-dnepr">Днепр</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="city-bel" value="Белая Церковь" checked>
                            <label for="city-bel">Белая Церковь</label>
                        </div>
                    </div>
                </div>

                <div class="section">
                    <div class="section-title">📊 Группировка</div>
                    <div class="checkbox-group">
                        <div class="checkbox-item">
                            <input type="checkbox" id="group-city" value="Город" checked>
                            <label for="group-city">Город</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="group-shop" value="Магазин">
                            <label for="group-shop">Магазин</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="group-type" value="Тип">
                            <label for="group-type">Тип</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="group-date" value="Дата">
                            <label for="group-date">Дата</label>
                        </div>
                    </div>
                </div>

                <button class="btn" id="generateBtn" onclick="generateReport()">
                    🔄 Сгенерировать отчет
                </button>
                
                <button class="btn btn-secondary" onclick="clearFilters()">
                    🗑️ Сбросить фильтры
                </button>
            </div>

            <div class="content-area">
                <div class="results-section">
                    <div class="results-title">📊 Результаты анализа</div>

                    <div class="stats-cards" id="statsCards">
                        <!-- Динамически заполняется -->
                    </div>

                    <div class="table-container">
                        <div id="resultsContent">
                            <div class="no-data">
                                Выберите параметры и нажмите "Сгенерировать отчет"
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        function calculateStats(data) {
            const total = data.reduce((sum, item) => sum + item.сумма, 0);
            const totalQty = data.reduce((sum, item) => sum + item.количество, 0);
            const avgCheck = data.length > 0 ? total / data.length : 0;
            
            return {
                total: total.toLocaleString('ru-RU', { minimumFractionDigits: 2 }),
                totalQty: totalQty.toLocaleString('ru-RU', { minimumFractionDigits: 1 }),
                count: data.length.toLocaleString('ru-RU'),
                avgCheck: avgCheck.toLocaleString('ru-RU', { minimumFractionDigits: 2 })
            };
        }

        function groupData(data, groupBy) {
            if (groupBy.length === 0) return data;

            const grouped = {};
            
            data.forEach(item => {
                const key = groupBy.map(field => {
                    const fieldMap = {
                        'Город': 'город',
                        'Магазин': 'магазин',
                        'Тип': 'тип',
                        'Дата': 'дата'
                    };
                    return item[fieldMap[field]] || '';
                }).join(' | ');

                if (!grouped[key]) {
                    grouped[key] = {
                        key: key,
                        items: [],
                        количество: 0,
                        сумма: 0
                    };
                }

                grouped[key].items.push(item);
                grouped[key].количество += item.количество;
                grouped[key].сумма += item.сумма;
            });

            return Object.values(grouped);
        }

        function createTable(data, groupBy) {
            let html = '<table>';
            
            // Заголовки
            html += '<thead><tr>';
            if (groupBy.length > 0) {
                html += '<th>Группировка</th>';
            } else {
                html += '<th>Город</th><th>Магазин</th><th>Тип</th><th>Дата</th>';
            }
            html += '<th>Количество</th><th>Сумма</th>';
            html += '</tr></thead>';

            // Данные
            html += '<tbody>';
            data.forEach(row => {
                html += '<tr>';
                
                if (groupBy.length > 0) {
                    html += `<td><strong>${row.key}</strong></td>`;
                } else {
                    html += `<td>${row.город || ''}</td>`;
                    html += `<td>${row.магазин || ''}</td>`;
                    html += `<td>${row.тип || ''}</td>`;
                    html += `<td>${row.дата || ''}</td>`;
                }

                html += `<td>${(row.количество || 0).toLocaleString('ru-RU', { minimumFractionDigits: 1 })}</td>`;
                html += `<td>${(row.сумма || 0).toLocaleString('ru-RU', { minimumFractionDigits: 2 })}</td>`;
                
                html += '</tr>';
            });
            html += '</tbody></table>';

            document.getElementById('resultsContent').innerHTML = html;
        }

        async function generateReport() {
            const generateBtn = document.getElementById('generateBtn');
            generateBtn.disabled = true;
            generateBtn.textContent = '⏳ Загрузка...';
            
            document.getElementById('resultsContent').innerHTML = '<div class="loading">Генерация отчета из реальных данных...</div>';
            
            try {
                const reportType = document.getElementById('reportType').value;
                const startDate = document.getElementById('startDate').value;
                const endDate = document.getElementById('endDate').value;
                const selectedCities = Array.from(document.querySelectorAll('input[id^="city-"]:checked')).map(input => input.value);
                const groupBy = Array.from(document.querySelectorAll('input[id^="group-"]:checked')).map(input => input.value);

                const response = await fetch('/api/generate_report', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        report_type: reportType,
                        start_date: startDate,
                        end_date: endDate,
                        cities: selectedCities,
                        group_by: groupBy
                    })
                });

                const result = await response.json();

                if (result.error) {
                    document.getElementById('resultsContent').innerHTML = `<div class="error">${result.error}</div>`;
                    return;
                }

                const data = result.data || [];
                
                // Вычисляем статистику
                const stats = calculateStats(data);
                
                // Отображаем статистические карточки
                const statsCards = document.getElementById('statsCards');
                statsCards.innerHTML = `
                    <div class="stat-card">
                        <div class="stat-value">${stats.count}</div>
                        <div class="stat-label">Записей</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${stats.total}</div>
                        <div class="stat-label">Общая сумма</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${stats.totalQty}</div>
                        <div class="stat-label">Общее количество</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${stats.avgCheck}</div>
                        <div class="stat-label">Среднее значение</div>
                    </div>
                `;

                // Группируем данные
                const grouped = groupData(data, groupBy);
                
                // Создаем таблицу
                createTable(grouped, groupBy);

            } catch (error) {
                document.getElementById('resultsContent').innerHTML = `<div class="error">Ошибка подключения: ${error.message}</div>`;
            } finally {
                generateBtn.disabled = false;
                generateBtn.textContent = '🔄 Сгенерировать отчет';
            }
        }

        function clearFilters() {
            document.getElementById('reportType').value = 'sales';
            document.getElementById('startDate').value = '2025-06-01';
            document.getElementById('endDate').value = '2025-06-03';
            
            document.querySelectorAll('input[id^="city-"]').forEach(input => input.checked = true);
            document.querySelectorAll('input[id^="group-"]').forEach(input => input.checked = false);
            document.getElementById('group-city').checked = true;
            
            document.getElementById('resultsContent').innerHTML = '<div class="no-data">Выберите параметры и нажмите "Сгенерировать отчет"</div>';
            document.getElementById('statsCards').innerHTML = '';
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Главная страница с интерфейсом"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/generate_report', methods=['POST'])
def api_generate_report():
    """API для генерации отчетов"""
    try:
        data = request.get_json()
        
        report_type = data.get('report_type', 'sales')
        start_date = data.get('start_date', '2025-06-01')
        end_date = data.get('end_date', '2025-06-03')
        selected_cities = data.get('cities', [])
        
        if report_type == 'sales':
            result = analyzer.generate_sales_report_api(start_date, end_date, selected_cities)
        elif report_type == 'losses':
            result = analyzer.generate_losses_report_api(start_date, end_date, selected_cities)
        else:
            result = {"error": "Неизвестный тип отчета"}
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": str(e)})

def open_browser():
    """Открывает браузер с задержкой"""
    time.sleep(1.5)
    webbrowser.open('http://127.0.0.1:5000')

def main():
    """Главная функция программы"""
    print("=" * 70)
    print("🚀 FTP ANALYZER v7.0 - ВЕБ-ПРИЛОЖЕНИЕ")
    print("=" * 70)
    print("Автор: Система аналитики данных\n")
    
    try:
        # Проверяем наличие необходимых библиотек
        try:
            import pandas as pd
            import openpyxl
        except ImportError as e:
            print("❌ Отсутствуют необходимые библиотеки!")
            print("Установите их командами:")
            print("pip install pandas openpyxl flask")
            return
        
        print("🌐 Запускаем веб-сервер...")
        print("📁 Рабочая папка:", analyzer.base_dir.absolute())
        print("🔗 Адрес: http://127.0.0.1:5000")
        print("\n⚡ Веб-интерфейс откроется автоматически...")
        print("❌ Для остановки нажмите Ctrl+C\n")
        
        # Открываем браузер в отдельном потоке
        threading.Thread(target=open_browser, daemon=True).start()
        
        # Запускаем Flask приложение
        app.run(host='127.0.0.1', port=5000, debug=False)
                
    except KeyboardInterrupt:
        print("\n\n⏹️  Веб-сервер остановлен")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")

if __name__ == "__main__":
    main()