#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FTP Analyzer - Этап 1: Сканирование структуры FTP-сервера
Автор: Система аналитики данных
Версия: 1.0
"""

import ftplib
import os
import json
import sys
from datetime import datetime
from pathlib import Path

class FTPAnalyzer:
    def __init__(self):
        # Настройки FTP-сервера
        self.ftp_host = "smkft.space"
        self.ftp_port = 2122
        self.ftp_user = "nielsen"
        self.ftp_pass = "Qazwsx32123"
        
        # Настройки локальных папок
        self.base_dir = Path("ftp_data")
        self.cache_dir = self.base_dir / "cache"
        self.structure_file = self.base_dir / "ftp_structure.json"
        
        # Создаем необходимые папки
        self.create_directories()
        
    def create_directories(self):
        """Создает необходимые папки для работы программы"""
        try:
            self.base_dir.mkdir(exist_ok=True)
            self.cache_dir.mkdir(exist_ok=True)
            print(f"✓ Создана рабочая папка: {self.base_dir.absolute()}")
            print(f"✓ Создана папка кеша: {self.cache_dir.absolute()}")
        except Exception as e:
            print(f"✗ Ошибка создания папок: {e}")
            sys.exit(1)
            
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
            
    def scan_directory(self, ftp, path="/"):
        """Сканирует директорию на FTP-сервере"""
        items = []
        try:
            print(f"🔍 Сканируем папку: {path}")
            
            # Получаем список элементов в папке
            ftp_items = []
            ftp.retrlines('LIST ' + path, ftp_items.append)
            
            for item in ftp_items:
                # Парсим строку LIST (Unix формат)
                parts = item.split()
                if len(parts) >= 9:
                    permissions = parts[0]
                    name = ' '.join(parts[8:])  # Имя может содержать пробелы
                    
                    # Пропускаем текущую и родительскую папки
                    if name in ['.', '..']:
                        continue
                        
                    # Определяем тип элемента
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
                # Добавляем CSV файлы
                structure['children'].append({
                    'name': item['name'],
                    'path': item['path'],
                    'type': 'file',
                    'size': item['size'],
                    'format': 'csv'
                })
                print(f"  📄 CSV файл: {item['name']} ({item['size']} байт)")
                
            elif item['type'] == 'directory':
                # Рекурсивно сканируем подпапки
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
            
    def load_structure(self):
        """Загружает структуру FTP из JSON файла"""
        try:
            if self.structure_file.exists():
                with open(self.structure_file, 'r', encoding='utf-8') as f:
                    structure = json.load(f)
                print(f"✓ Структура загружена из файла: {self.structure_file.absolute()}")
                return structure
            else:
                print(f"⚠️  Файл структуры не найден: {self.structure_file.absolute()}")
                return None
        except Exception as e:
            print(f"✗ Ошибка загрузки структуры: {e}")
            return None
            
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
                    
    def run_stage1(self):
        """Запускает первый этап - сканирование структуры FTP"""
        print("=" * 60)
        print("🚀 ЭТАП 1: СКАНИРОВАНИЕ СТРУКТУРЫ FTP-СЕРВЕРА")
        print("=" * 60)
        
        # Подключаемся к FTP
        ftp = self.connect_ftp()
        if not ftp:
            print("❌ Не удалось подключиться к FTP-серверу")
            return False
            
        try:
            # Сканируем структуру
            print("\n🔍 Начинаем сканирование структуры...")
            structure = self.scan_ftp_structure(ftp, "/www", max_depth=3)
            
            # Закрываем соединение
            ftp.quit()
            print("✓ Соединение с FTP закрыто")
            
            # Сохраняем структуру
            if self.save_structure(structure):
                print("\n📋 КРАТКАЯ СВОДКА СТРУКТУРЫ:")
                print("-" * 40)
                self.print_structure_summary(structure)
                
                print(f"\n✅ ЭТАП 1 ЗАВЕРШЕН УСПЕШНО!")
                print(f"📁 Рабочая папка: {self.base_dir.absolute()}")
                print(f"📄 Файл структуры: {self.structure_file.absolute()}")
                print("\n💡 Для перехода к следующему этапу напишите 'ОК'")
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
    print("FTP Analyzer v1.0 - Анализатор данных FTP-сервера")
    print("Автор: Система аналитики данных\n")
    
    try:
        analyzer = FTPAnalyzer()
        success = analyzer.run_stage1()
        
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