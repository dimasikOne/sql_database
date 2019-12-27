Заливаем файлы в hadoop:
hadoop fs - put doc1.txt /user/usertest/Sud
hadoop fs - put doc2.txt /user/usertest/Sud
hadoop fs - put doc3.txt /user/usertest/Sud
hadoop fs - put doc4.txt /user/usertest/Sud
hadoop fs - put doc5.txt /user/usertest/Sud
hadoop fs - put doc6.txt /user/usertest/Sud


делаем таблицу из файлов:
 create external table book (text_line String)
    > row format delimited
    > create external table bookS (text_line String)
    > row format delimited
    > location '/user/usertest/Sud/'

load data inpath '/user/usertest/Sud/'

Чистим мусор:
create table clean_books as 
    select split_line, count(*) COL, INPUT__FILE__NAME as filename
    from bookS2
    lateral view outer explode(split(line,'[^A-Za-zА-Яа-я0-9-]+')) t1 as split_line
    group by split_line, INPUT__FILE__NAME

Создаем таблицы для каждого файла, чтобы посчитать коэфт TF:
create table doc1 as    
    select split_line, COL from clean_books where filename ='hdfs://cdh631.itfbgroup.local:8020/user/usertest/Sud/doc1.txt'

create table doc2 as 
    select split_line, COL from clean_books where filename ='hdfs://cdh631.itfbgroup.local:8020/user/usertest/Sud/doc2.txt'

create table doc3 as 
    select split_line, COL from clean_books where filename ='hdfs://cdh631.itfbgroup.local:8020/user/usertest/Sud/doc3.txt'

create table doc4 as 
    select split_line, COL from clean_books where filename ='hdfs://cdh631.itfbgroup.local:8020/user/usertest/Sud/doc4.txt'

create table doc5 as 
    select split_line, COL from clean_books where filename ='hdfs://cdh631.itfbgroup.local:8020/user/usertest/Sud/doc5.txt'

create table doc6 as 
    select split_line, COL from clean_books where filename ='hdfs://cdh631.itfbgroup.local:8020/user/usertest/Sud/doc6.txt'
    
    Создаем таблицу для подсчета коэф-тов IDF:
create table IDf as
    select sum(col) as sum_col, split_line from clean_books group by split_line

	Перезаписываем таблицы с подсчитанными коэфтами TF:
    
    insert overwrite table doc1 select log(col/sum(col)) as TF, split_line from doc1 group by split line
    
    insert overwrite table doc2 select log(col/sum(col)) as TF, split_line from doc2 group by split line
    
    insert overwrite table doc3 select log(col/sum(col)) as TF, split_line from doc3 group by split line
    
    insert overwrite table doc4 select log(col/sum(col)) as TF, split_line from doc4 group by split line
    
    insert overwrite table doc5 select log(col/sum(col)) as TF, split_line from doc5 group by split line
    
    insert overwrite table doc6 select log(col/sum(col)) as TF, split_line from doc6 group by split line
    

	Выводим итоговый коэфт:
    select log(IDF.sum_col/IDF.sum(sum_col))*doc1.TF from TF, IDF where IDF.split_line=TF.split_line
    