//
//Автор - Sergey Popurey  
//Парсер текстовых файлов для извлечения значений строк в составе которых присутствуют заданные для поиска префиксы.
//
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
//parserfile.exe -fkey filekey.txt -dir .\1\
var mydir = flag.String("mydir", `.\1\`, `Каталог размещения файлов для парсинга, например -mydir .\1\`)
//var fkey  = flag.String("fkey", ``, `Файл задание, список ключей для поиска, например -fkey filekey.txt`)
var wg sync.WaitGroup

//Структура соответствующая поиску значений строк
type  mydata struct{
	host_name string
	address string
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
		panic(err)
	}
	defer floger.Close()

	w := newWords()

	log.SetOutput(floger)
	t0 := time.Now()
	log.Printf("СТАРТ %v \n", t0)

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
			myParser(fl, mfile, w)
			defer fl.Close()
			defer wg.Done()
		}(fl, file)
	}
	wg.Wait()

	for word, vl := range w.found {
			fmt.Printf("%s;%s;%s\n", word, vl.host_name,vl.address)
	}


	t1 := time.Now()
	log.Printf("Успешное завершение работы, время выполнения %v сек.\n", t1.Sub(t0))
}

// func myParser - функция поиска значений строк в файле в составе которых присутствует искомое значение префиксов, соответствующих полям структуры mydata
// param - fl *os.File - указатель на файл с данными, 
// mfile string - значение имени файла, оноже является значением ключа в словаре для хранения результатов поиска, 
// dict *words - указатель на словарь для хранения результатов поиска значений
func myParser(fl *os.File, mfile string, dict *words) mydata {
	var md mydata
        structType := reflect.TypeOf(mydata{})
        //_struct := reflect.ValueOf(md)

	scanner := bufio.NewScanner(fl)
	row := ""
	//Сканируем строки файла и ищем строки в составе которых присутствуют искомые префиксы 
	for scanner.Scan() {
		// запишем строку из файла в переменную row и обрежим пробелы
		row = strings.TrimSpace(scanner.Text())
                // Заполним структуру mydata по найденным соответствующим значениям префиксов строк 
                if strings.HasPrefix(row, structType.Field(0).Name)  { //"host_name"
                      md.host_name = strings.TrimSpace(strings.TrimPrefix(row, structType.Field(0).Name))
		}
                if strings.HasPrefix(row, structType.Field(1).Name)  { //"address"
                      md.address = strings.TrimSpace(strings.TrimPrefix(row, structType.Field(1).Name))
		}
		// Или общий случай с применением рефлексии
                //for i := 0; i < _struct.NumField(); i++ {
                //      name := structType.Field(i).Name
                //      //fmt.Println(name, _struct.Field(i))
                //     if strings.HasPrefix(row, name)  { //"host_name" или "address" или иное значение имени поля структуры
                //         _struct.Field(i) = strings.TrimSpace(strings.TrimPrefix(row, name))
	        //	}

                //}
	}
      // Запись в словарь результатов поиска значений
      dict.add( mfile, md)
      return md
     }
	      