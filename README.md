# Fraud detection system
Система классификации мошеннических банковских транзакций


## Цели системы:
1. Конфиденциальность системы (все данные должны быть обезличены, а каналы связи защищены, https);
2. False Positive Rate (FPR) системы должен быть меньше 0.05 (бизнес требования заказчика);
Не более 2 FN на 100 транзакций, т.е. на выходе из алгоритма должно быть не более 2 мошеннических операций из 100, классифицированных как легитимные транзакции (требования заказчика).
3. Общие суммарные потери клиентов в месяц из-за мошеннических операций не более 500 тыс.руб;
4. Система должна работать в real-time.
5. Отказоустойчивость (распределенная система).
6. Пропускная способность системы (нагрузка: в среднем 50 * 1.5 транзакций в сек, накануне праздников 400 * 1.5 транзакций / сек).
7. Срок проекта: бейзлайн - 3 мес, конечное решение - 3 мес. Итого 6 мес.
8. Стоимость системы не больше 10 млн.руб;
9. Система должна быть развернута в облаке, так как заказчик не готов размещать сервис на своих мощностях (в облаке достигается высокая надежность за счет горизонтальной масштабируемости и хорошо настроенной инфраструктуры со стороны провайдера облачного сервиса).


## Функция ошибки и метрики качества ML модели:
Задачу детекции фрод транзакций будем решать как задачу классификации.  
### Функция ошибки:  
В качестве функции ошибки, которую необходимо минимизировать возьмем **Focal loss** со взвешиванием по соотношению классов (добавляем коэффициент с долей класса):  

$FL(p_{t}) = -&#945_{c}(1 - p_{t})^&#947*log(p_{t})$  

Эта функция ошибки поможет в борьбе с дисбалансом классов (очевидно, что мошеннических транзакций значительно меньше, чем легитимных). Эта     функция ошибки снижает вес ошибки на объектах, на которых модель имеет более высокую уверенность, тем самым позволяя вносить больший вклад     тем немногочисленным объектам с положительным классом, на которых модель не уверена.

### Метрики качества:  
При выборе модели и поиске гиперпараметров будем смотреть на ROC AUC и PR AUC, а также настраивать модель под бизнес требования FPR (ложные срабатывания) <= 5%, и не более 2% мошеннических операций среди всех проверенных операций у заказчика (не ясно, из условия задачи, какую метрику описывает последнее условие). Есть подозрения, что при сильном дисбалансе классов ROC AUC будет непрезентабельна, но с другой стороны PR AUC имеет свойство изменяться при изменении баланса классов. В общем, посмотрим в деле.


## Архитектура решения
1. Используем Kafka для сообщения с клиентом через HTTPS протокол;
2. Antifrod API service получает сообщения потоковой обработки от Kafka через HTTPS протокол;
3. Apache Spark (для обезличивания, шифрования, расчета и генерации признаков);
4. Записываем полученные признаки, актуальные статистики в Feature-store;
5. Вызываем ML модель для прогнозирования и получаем предсказания;
6. Записываем логи с результами предсказания в s3 object-storage;
7. Отправляем результаты в Kafka;
8. Kafka возвращает результаты клиенту.

9. Также необходимо рассмотреть переобучение модели из данных с обновленными данными и признаками из feature-stor.
10. А также рассмотреть возможность обратной связи клиента по результатам.

## Объектное хранилище данных для обучения
Ссылка на S3 бакет с данными: s3://otus-mlops-bucket/fraud-data/  

Изначально данные находятся в бакете "s3://mlops-data/fraud-data/". Чтобы скопировать данные в нужный бакет необходимо в s3cmd ввести команду:  
s3cmd sync s3://mlops-data/fraud-data/  s3://otus-mlops-bucket/fraud-data/ --acl-public

## Перенос данных на кластер с файловой системой HDFS
Подключаясь к мастер-ноде кластера с HDFS запускаем там команду hadoop distcp для копирования файлов в распределенную файловую систему HDFS. 
Происходит размещение ~113 Гб данных на дисках data-нод с фактором репликации 1.  
  
Для копирования нужно ввести команду (из документации yandex cloud):  
  
hadoop distcp \\  
  -D fs.s3a.bucket.dataproc-examples.endpoint=storage.yandexcloud.net \\  
  -D fs.s3a.bucket.dataproc-examples.access.key=<access_key> \\  
  -D fs.s3a.bucket.dataproc-examples.secret.key=<secret_key> \\  
  -update \\  
  -skipcrccheck \\  
  -numListstatusThreads 10 \\  
  s3a://<бакет> \\  
  hdfs://<хост HDFS>/<путь>/  

Или если бакет публичный:  
hadoop distcp  s3a://<бакет> /<путь>/  

Скриншот объектов в целевой директории файловой системы HDFS, где хранятся данные для обучения:
![image](https://github.com/boringType/fraud_detection/assets/122883035/5ffc1a38-c4db-4c0b-9873-7e30e9232eef)

## Оценка затрат на поддержание spark-кластера  
Spark-кластер состоит из одной Мастер ноды (2vCPU, 8гб RAM, 40гб ssd диск), 3-х дата-нод (4vCPU, 16гб RAM, 128гь hdd диск). Также есть затраты на публичный ip-адрес для мастер ноды и затраты на поддержание dataproc инфраструктуры между хостами кластера.
Согласно калькулятору yandex cloud получаем 4120 руб/мес для поддержания мастер-ноды (с учетом затрат на публичный ip-адрес и поддержания инфраструктуры кластера "dataproc" затраты) и 22000 руб/мес для поддержания 3-х дата-нод (также с учетом затрат для поддержания инфраструктуры dataproc, но без публичного ip-адреса). Итого получаем 26120 руб/мес (без учета object storage), если округлить, то примерно 26500-30000 руб/мес.
Для сравнения стоимость object storage (s3 bucket) со стандартным хранением с объемом 200ГБ (объем наших данных для обучения 123гб) стоит 400 руб/мес, а объемом 3*128гб как в дата-нодах = 770 руб/мес, а 424гб (как в сумме с мастер-нодой) в месяц обойдется в 850-900 руб.

## Способы оптимизации работы spark-кластера
1. Если не критично, то во время низких нагрузок на кластер (ночью количество транзакций сравнительно невысокое) можно виртуальные машины делать прерываемыми (насколько это критично не могу понять из-за отсутствия опыта). Но это довольно сильно экономит средства:
   8120 руб/мес (для 3х дата-нод) + 1816 руб/мес для мастер-ноды = 9936 руб/мес ~ 10000-11000 руб/мес.
2. Также можно сочетать уменьшение количества ядер на ВМ дата-нод, уменьшение RAM 16гб до 8гб, а также снижение гарантированной доли загрузки vCPU до 50%.
3. Пока изучается :)

