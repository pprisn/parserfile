//Парсер текстовых файлов для извлечения значений строк в составе которых в префиксе присутствуют заданные 
//для поиска ключи, перечисленные в файле questkey.txt
//Автор - pprisn@yandex.ru Sergey Popurey  
package main

import (
	"flag"
	"fmt"
        "bufio"
	"log"
	"os"
	"path/filepath"
	"time"
	"sync"
        "strings"
	"reflect"
)


//Пример запуска приложения с указанными параметрами
//parserfile.exe -fquest questkey.txt -dir D:\go_workspace\src\github.com\pprisn\parserfile\1\  > cfg.csv
var mydir = flag.String("mydir"   , `C:\data\`         , `Каталог размещения файлов для парсинга, например -mydir C:\data\`)
var fquest  = flag.String("fquest", `questkey.txt` , `Файл задание, список ключей для поиска, например -fquest questkey.txt`)
// 
var wg sync.WaitGroup

//Шаблон для хранения ключей
var quest mydata

//Структура соответствующая поиску значений строк из файла задания questkey.txt
// 
type  mydata struct{
//	Host_name string `host_name`
//	Address   string `address`
	Field0    string ``
	Field1    string ``
	Field2    string ``
	Field3    string ``
	Field4    string ``
	Field5    string ``
	Field6    string ``
	Field7    string ``
	Field8    string ``
	Field9    string ``
	Field10   string ``
	Field11   string ``
        Field12   string ``
}

//структура для хранения результатов
type words struct {
	sync.RWMutex //добавить в структуру мьютекс
	found        map[string]mydata
}

//Инициализация области памяти
func newWords() *words {
	return &words{found: map[string]mydata{}}
}

//Фиксируем вхождение слова
//заводим новый элемент или дабавляем к значению элемента найденнай новый параметр через разделитель ;
func (w *words) add(word string, WS mydata) {
	w.Lock()         //Заблокировать объект
	defer w.Unlock() // По завершению, разблокировать
	_, ok := w.found[word]
	if !ok { //т.е. если word запроса не найдено заводим новый элемент слайса
		w.found[word] = WS
		return
	}
	// слово найдено в очередной раз , запишем в лог не стандартную ситуацию
	log.Printf("Повтор чтения файла %s \n", word)
	//w.found[word] = Fvalue + ";" + WS
	return
}

// func main - главная функция
// выполняется заполнение слайса filelist значениями имен файлов которые необходимо распарсить, получить содержимое строк которые содержат 
// префиксы соответствующие значениям структуры mydata
// Чтение файлов выполняется с применением параллелного запуска функции myParser
// Запись найденных значений в структуру words выполняется с синхронизацией доступа 
// в горутинах к разделяемому ресурсу words с помощью мьютексов.  
func main() {
	var err error
	flag.Parse()
	var floger *os.File
	if floger, err = os.OpenFile("parserfile.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		log.Fatal(err)
	}
	defer floger.Close()

        var fq *os.File
        if fq, err = os.Open(*fquest); err != nil {
	   log.Fatal(err)
	}
        defer fq.Close()

	w := newWords()

	log.SetOutput(floger)
	t0 := time.Now()
	log.Printf("СТАРТ %v \n", t0)

        myQuest(fq, &quest)

        //log.Printf(" %+v \n", quest)
        //return
 
	// Массив для хранения списка файлов
	fileList := []string{}
	var md, mf string
	err = filepath.Walk(*mydir, func(path string, f os.FileInfo, err error) error {
		// проверим чтобы список формировался из файлов расположенных в заявленном каталоге
		md, mf = filepath.Split(path)
		// fmt.Println(mfile)
		l, _ := filepath.Match(*mydir+"*.cfg", path)
		if (md == *mydir) && (l == true) {
			fileList = append(fileList, path)
		}
		return nil
	})


        for _, file := range fileList {
		log.Println("Файл в очередь на проверку", file)
		var fl *os.File
		if fl, err = os.Open(file); err != nil {
			panic(err)
		}
		
		wg.Add(1)
		go func(fl *os.File, mfile string) {
			myParser(fl, mfile, w, &quest)
			defer fl.Close()
			defer wg.Done()
		}(fl, file)
	}
	wg.Wait()

	//Шапка csv файла
	fmt.Printf("fileName;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s\n", 
                                     quest.Field0,quest.Field1,quest.Field2,quest.Field3,quest.Field4,
                                     quest.Field5,quest.Field6,quest.Field7,quest.Field8,quest.Field9,
                                     quest.Field10,quest.Field11,quest.Field12)
	//Данные csv файла
        for word, vl := range w.found {
			fmt.Printf("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s\n", 
                                    word, vl.Field0,vl.Field1,vl.Field2,vl.Field3,vl.Field4,
                                          vl.Field5,vl.Field6,vl.Field7,vl.Field8,vl.Field9,
                                          vl.Field10,vl.Field11,vl.Field12)
	}

	t1 := time.Now()
	log.Printf("Успешное завершение работы, время выполнения %v сек.\n", t1.Sub(t0))
}

//func myQuest - функция читает файл с заданными ключами поиска и присваивает тегам переменной структуры mydata соответствующие значения.
//param fq *os.File - указатель на файл questkey.txt, q - указатель на переменную quest структуры mydata
//Пустые строки в файле questkey.txt игнорируются 
func myQuest(fq *os.File, q *mydata) {
  scanner := bufio.NewScanner(fq)
  val := reflect.ValueOf(q).Elem()
  row := ""
  i :=0
  for scanner.Scan(){
    row = strings.TrimSpace(scanner.Text())
    if row != "" {
      val.Field(i).SetString(row)    
      i++
      if i > val.NumField() { break }
    }
  }
}


// func myParser - функция поиска значений строк в файле в составе которых присутствует искомое значение префиксов, соответствующих полям структуры mydata
// param - fl *os.File - указатель на файл с данными, 
// mfile string - значение имени файла, оноже является значением ключа в словаре для хранения результатов поиска, 
// dict *words - указатель на словарь для хранения результатов поиска значений
func myParser(fl *os.File, mfile string, dict *words, q *mydata) mydata {

        var md = mydata{}
        //structType := reflect.TypeOf(mydata{})
        val := reflect.ValueOf(&md).Elem()
        qval := reflect.ValueOf(q).Elem()

        scanner := bufio.NewScanner(fl)
	row := ""
	//Сканируем строки файла и ищем строки в составе которых присутствуют искомые префиксы 
	for scanner.Scan() {
		// запишем строку из файла в переменную row и обрежим пробелы
		row = strings.TrimSpace(scanner.Text())
                // Заполним структуру mydata по найденным соответствующим значениям префиксов строк 
                // время выполнения 70.3ms сек.
             //   if strings.HasPrefix(row, "host_name")  { //"host_name"
             //         md.Host_name = strings.TrimPrefix(row, "host_name")
	     //     }
             //   if strings.HasPrefix(row,"address")  { //"address"
             //         md.Address = strings.TrimPrefix(row, "address")
             //    }
		// Или общий случай с применением рефлексии, ключи записаны в теге переменных структуры
                // время выполнения 934.3213ms сек.
               // for i := 0; i < val.NumField(); i++ {
	       //      typeField := val.Type().Field(i)
               //       tag := fmt.Sprintf("%v",typeField.Tag)
               //      if strings.HasPrefix(row, tag)  { //"Host_name" или "Address" или иное значение имени поля структуры
               //          val.Field(i).SetString(strings.TrimPrefix(row, tag))
	       // 	}                                                                               
               // }

		// Или общий случай с применением рефлексии, ключи записаны как значения переменной quest
                // время выполнения 123.13ms сек.
                for i := 0; i < qval.NumField(); i++ {
	     	      //typeField := val.Type().Field(i)
                     tag := fmt.Sprintf("%v",qval.Field(i))
                     if tag == "" { break }
                     if strings.HasPrefix(row, tag)  { //"host_name" или "address" или иное значение ключа
                         row = strings.TrimPrefix(row, tag)
                         val.Field(i).SetString(strings.TrimSpace(row))
	             }                                                                               
                }

	}

      // Запись в словарь результатов поиска значений
      dict.add( mfile, md)
      return md
     }
	      