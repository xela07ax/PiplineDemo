package main

import (
	"PiplineConveer/pipeline"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// MD5Result структура для передачи результатов
type MD5Result struct {
	PathRoot string
	Filepath string
	Hash     [16]byte
	Err      error
	Gopher   int
	Task     int
}

const PARALLELISM_DEGREE = 10

var ParallelismDegreeResult int

const (
	WALKER    = "Walker"
	SUMMER    = "Summer"
	COLLECTOR = "Collector"
)

func main() {
	// Начальное время
	startTime := time.Now()
	// os.Args[1] — первый аргумент путь к директории
	// os.Args[2] — второй аргумент количество обработчиков (необязательно)

	// Проверяем, достаточно ли аргументов
	// если аргумент число, то это количество рутин
	ParallelismDegreeResult = PARALLELISM_DEGREE
	rootDirs := make(map[string]struct{})
	if len(os.Args) > 1 {
		for _, arg := range os.Args[1:] {
			try, err := strconv.Atoi(arg)
			if err != nil {
				// значит путь к папке
				rootDirs[arg] = struct{}{}
			} else {
				// если получилось преобразовать, значит рутин
				ParallelismDegreeResult = try
			}
		}
	} else {
		log.Fatalf("Пожалуйста, предоставьте как минимум один аргумента командной строки. Ожидается путь к папке и количество обработчиков\nПример: go run main.go 4 D:/Downloads/nnm2")
	}

	// Запускаем пайплайн
	p := pipeline.NewPipeline()

	// 1. Создаем ноды
	p.AddNode(WALKER, FileWalkerNode, 2)
	p.AddNode(SUMMER, MD5SummerNode, ParallelismDegreeResult)
	p.AddNode(COLLECTOR, CollectorNode, 1)

	log.Printf("Запуск вычисления MD5 хешей с параллелизмом %d...", ParallelismDegreeResult)
	// 4. Запускаем
	p.Run()

	// 5. Поставим задачи на выполнение
	for path := range rootDirs {
		log.Printf("Директория:%s...", path)
		p.Inputs[WALKER] <- [2]interface{}{path}
	}
	// 6. Ожидаем завершения
	log.Println("Ожидаем завершения")
	for a := 1; a <= len(rootDirs); a++ {
		errMaybe := <-p.Outputs[WALKER]
		if errMaybe[1] != nil {
			err, ok := errMaybe[1].(error)
			if !ok {
				log.Fatalf("Ошибка данных error в модуле main Walker:%v", errMaybe[1])
			}
			log.Printf("Ошибка при выполнении:%v", err)
			// Экстренно останавливаем все задачи
			err = p.Kill()
			log.Printf("Killed output: %v", err)
		}
	}
	log.Println("Все файлы обработаны. Пайплайн завершен.")
	// Конечное время
	endTime := time.Now()
	// Подсчет разницы во времени
	duration := endTime.Sub(startTime)

	log.Printf("Время выполнения: %s", duration)
}

// FileWalkerNode рекурсивно обходит директорию и отправляет пути файлов
func FileWalkerNode(gopher int, dirTask [2]interface{}, inputs, outputs map[string]chan [2]interface{}) {
	var err error
	var files []string // сделаем список обрабатываемых файлов

	path, ok := dirTask[0].(string)
	if !ok {
		log.Fatalf("Ошибка данных string в модуле pipeline FileWalkerNode:%v", dirTask[0])
	}
	err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		dirTask[1] = fmt.Errorf("ошибка при обходе директории: %v", err)
		outputs[WALKER] <- dirTask
	}
	// Создадим ожидащий завершения остальных операций канал
	waitTempWalkerChannelName := fmt.Sprintf("%s_%s", WALKER, path)
	outputs[waitTempWalkerChannelName] = make(chan [2]interface{})

	for _, file := range files {
		inputs[SUMMER] <- [2]interface{}{MD5Result{
			PathRoot: path,
			Filepath: file,
			Task:     gopher,
		}}
		// test error
		//if i == 4 {
		//	dirTask[1] = fmt.Errorf("123[fake test]ошибка при обходе директории: %v", err)
		//	outputs[WALKER] <- dirTask
		//}
	}
	// Ждем, пока все файлы будут обработаны
	// Собираем результаты из канала results
	for a := 1; a <= len(files); a++ {
		<-outputs[waitTempWalkerChannelName]
	}
	// очистим временный канал
	delete(outputs, waitTempWalkerChannelName)
	outputs[WALKER] <- [2]interface{}{path}
}

// MD5SummerNode вычисляет MD5 хеши параллельно
func MD5SummerNode(gopher int, element [2]interface{}, inputs, outputs map[string]chan [2]interface{}) {
	pathData, ok := element[0].(MD5Result)
	if !ok {
		log.Fatalf("Ошибка данных string в модуле pipeline MD5SummerNode:%v", element[0])
	}
	sum, err := calculateMD5(pathData.Filepath)
	if err != nil {
		element[1] = fmt.Errorf("ошибка обработки файла %s: %v", pathData.Filepath, err)
		outputs[WALKER] <- element
		// Обратите внимание, при ошибке мы не отправляем отчет в Walker и он ожидая окончания зависнет навсегда!
		return
	}
	// Отправляем на обработку в коллектор для вывода результата в консоль
	pathData.Hash = sum
	pathData.Gopher = gopher
	element[0] = pathData
	inputs[COLLECTOR] <- element
}

// CollectorNode печатает результаты
func CollectorNode(gopher int, element [2]interface{}, inputs, outputs map[string]chan [2]interface{}) {
	md5Data, ok := element[0].(MD5Result)
	if !ok {
		log.Fatalf("Ошибка данных string в модуле MD5Result CollectorNode:%v", element[0])
	}
	fmt.Printf("TaskRoutine:%d|RoutineMd5:%d|%x  %s\n", md5Data.Task, md5Data.Gopher, md5Data.Hash, md5Data.Filepath)
	// Надо обо всех успешных операциях оповещать Walker, что бы корректно завершить работу
	outputs[fmt.Sprintf("%s_%s", WALKER, md5Data.PathRoot)] <- element
}

// calculateMD5 — вспомогательная функция для вычисления хеша файла
func calculateMD5(filePath string) ([16]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return [16]byte{}, err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return [16]byte{}, err
	}

	var result [16]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}
