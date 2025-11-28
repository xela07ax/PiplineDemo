package main

import (
	"PiplineConveer/Minions"
	"PiplineConveer/pipeline"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// MD5Result структура для передачи результатов
type MD5Result struct {
	Path   string
	Hash   [16]byte
	Err    error
	Gopher int
}

const PARALLELISM_DEGREE = 10

var ParallelismDegreeResult int

// --- Реализация нод (ProcessorFunc) ---

// FileWalkerNode рекурсивно обходит директорию и отправляет пути файлов
func FileWalkerNode(dirPath string) pipeline.ProcessorFunc {
	return func(in map[string]chan [4]interface{}, out map[string]chan [4]interface{}, wg *sync.WaitGroup) {
		defer wg.Done()
		var err error
		defer func() {
			out["files"] <- [4]interface{}{"nil", err, true}
		}()

		err = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				// Отправляем путь файла в выходной канал
				out["files"] <- [4]interface{}{MD5Result{path, [16]byte{}, nil, 0}, nil, nil}
			}
			return nil
		})

		if err != nil {
			err = fmt.Errorf("Ошибка при обходе директории: %v\n", err)
		}
	}
}

// MD5SummerNode вычисляет MD5 хеши параллельно
func MD5SummerNode(in map[string]chan [4]interface{}, out map[string]chan [4]interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out["results"]) // Закрываем выходной канал, когда все хеши посчитаны

	inputFiles := in["files"]
	results := out["results"]

	// Запускаем воркеры
	md5conveer := Minions.NewConveer("md5worker", func(gopherNumber int, element interface{}, out chan [4]interface{}) {
		result, ok := element.(MD5Result)
		if !ok {
			panic("Ошибка данных string в модуле Minions")
		}
		hash, err := calculateMD5(result.Path)
		result.Hash = hash
		result.Gopher = gopherNumber
		out <- [4]interface{}{result, err}
	})
	md5conveer.RunMinions(ParallelismDegreeResult)
	// Зададим для воркеров выходной канал pipiline-а
	// Отправляем результат в выходной канал
	md5conveer.OutputChan = results

	// Используем bounded concurrency (ограничение параллелизма)
	// Для каждого файла запускаем горутину
	for filePathFlow := range inputFiles { // 0- данные any MD5Result, 1-ошибка error, 2-завершено bool, 3 - Kill bool
		if filePathFlow[1] != nil {
			// ошибка
			md5conveer.Kill()
			results <- [4]interface{}{filePathFlow[0], filePathFlow[1], true}
			return
		}
		if filePathFlow[2] != nil {
			// завершение, все файлы обработаны
			// Ждем, пока все файлы будут обработаны
			md5conveer.Stop()
			return
		}
		// отправляем задание напрямую в обработчик
		md5conveer.InputChan <- filePathFlow[0]
	}
}

// CollectorNode собирает результаты и печатает их
func CollectorNode(in map[string]chan [4]interface{}, out map[string]chan [4]interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	// У этой ноды нет выходов, только вход "results"

	for resultFlow := range in["results"] { // 0- данные any, 1-ошибка error, 2-завершено bool, 3 - Kill bool
		if resultFlow[1] != nil {
			// ошибка
			fmt.Printf("Ошибка обработки %v: %v\n", resultFlow[0], resultFlow[1])
			return
		}
		md5res, ok := resultFlow[0].(MD5Result)
		if !ok {
			panic("Ошибка данных MD5Result в модуле pipeline CollectorNode")
		}
		if md5res.Err != nil {
			fmt.Printf("Ошибка обработки файла %s: %v\n", md5res.Path, md5res.Err)
		} else {
			fmt.Printf("%d|%x  %s\n", md5res.Gopher, md5res.Hash, md5res.Path)
		}
		if resultFlow[2] != nil {
			// завершено успешно
			return
		}
	}
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

// --- Точка входа ---

func main() {
	// Начальное время
	startTime := time.Now()
	// os.Args[1] — первый аргумент путь к директории
	// os.Args[2] — второй аргумент количество обработчиков (необязательно)

	// Проверяем, достаточно ли аргументов
	var err error
	if len(os.Args) > 2 {
		ParallelismDegreeResult, err = strconv.Atoi(os.Args[2])
		if err != nil {
			panic(fmt.Errorf("Неверный второй аргумент, ожидаем число %v\n", err))
		}
	} else if len(os.Args) > 1 {
		ParallelismDegreeResult = PARALLELISM_DEGREE
	} else {
		panic("\"Пожалуйста, предоставьте как минимум один аргумента командной строки. Ожидается путь к папке и количество обработчиков\"\n\"Пример: go run main.go D:/Downloads/nnm2 4\"")
	}

	rootDir := os.Args[1]

	p := pipeline.NewPipeline()

	// 1. Создаем ноды
	walker := pipeline.NewNode(
		"Walker",
		FileWalkerNode(rootDir),
		[]string{},        // Нет входов
		[]string{"files"}, // Один выходной порт "files"
	)

	summer := pipeline.NewNode(
		"Summer",
		MD5SummerNode,
		[]string{"files"},   // Один входной порт "files"
		[]string{"results"}, // Один выходной порт "results"
	)

	collector := pipeline.NewNode(
		"Collector",
		CollectorNode,
		[]string{"results"}, // Один входной порт "results"
		[]string{},          // Нет выходов
	)

	// 2. Добавляем ноды в пайплайн
	p.AddNode(walker)
	p.AddNode(summer)
	p.AddNode(collector)

	// 3. Соединяем ноды, формируя граф
	// Output "files" Walker'а -> Input "files" Summer'а
	if err := p.Connect(walker, summer, "files", "files"); err != nil {
		panic(err)
	}
	// Output "results" Summer'а -> Input "results" Collector'а
	if err := p.Connect(summer, collector, "results", "results"); err != nil {
		panic(err)
	}

	fmt.Printf("Запуск вычисления MD5 хешей в %s с параллелизмом %d...\n", rootDir, ParallelismDegreeResult)

	// 4. Запускаем и ждем завершения
	p.Run()
	p.Wait()

	fmt.Println("Все файлы обработаны. Пайплайн завершен.")
	// Конечное время
	endTime := time.Now()
	// Подсчет разницы во времени
	duration := endTime.Sub(startTime)

	fmt.Printf("Время выполнения: %s\n", duration)
}
