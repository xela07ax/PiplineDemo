package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ProcessorFunc определяет сигнатуру функции обработки данных в ноде.
// Она принимает входной канал и возвращает выходной канал.

type Conveer struct {
	Name        string
	InputChan   chan [2]interface{}
	OutputChan  chan [2]interface{}
	Inputs      map[string]chan [2]interface{}
	Outputs     map[string]chan [2]interface{} // 1-данные, 2-ошибка
	ProcessFunc ProcessorFunc
	process     *mutexRunner
	terminate   chan struct{} // Сигнал, что пока закругляться
	kill        chan struct{} // Принудительное завершение, сигнал брокеру
	stopX       chan error    //Миньон завершила свою работу
	Wait        chan struct{}
}

// combineErrors объединяет слайс ошибок в одну строку с разделителем "\n"
func combineErrors(errorsSlice []error) (err error) {
	if len(errorsSlice) == 0 {
		return
	}
	errorStrings := make([]string, len(errorsSlice))
	for i := range errorsSlice {
		errorStrings[i] = errorsSlice[i].Error()
	}
	errTxt := strings.Join(errorStrings, "\n")
	return errors.New(errTxt)
}

// Методы завершения задач
// Сценарий завершения 1, корректное завершение
// мы получили команду завершиться, дочитываем очередь и выходим
// Сценарий завершения 2, принудительное завершение
// мы получили команду принудительного завершения, выходим незамедлительно

func (m *Conveer) Kill() error {
	close(m.kill)
	var errorsSlice []error
	for i := 0; i < m.GetCores(); i++ {
		errRoutine := <-m.stopX
		if errRoutine != nil {
			errorsSlice = append(errorsSlice, errRoutine)
		}
	}
	return combineErrors(errorsSlice)
}

func (m *Conveer) Stop() {
	close(m.terminate)
	for i := 0; i < m.GetCores(); i++ {
		<-m.stopX
	}
}

func (m *Conveer) WaitStoper(terminate <-chan struct{}) {
	<-terminate
	m.Stop()
	m.Wait <- struct{}{}
	return
}

func NewConveer(node *Node, execFunc ProcessorFunc, in, out map[string]chan [2]interface{}) *Conveer {
	return &Conveer{
		Name:        node.Name,
		ProcessFunc: execFunc,
		Inputs:      in,
		Outputs:     out,
		InputChan:   in[node.Name],
		OutputChan:  out[node.Name],
		terminate:   make(chan struct{}),
		stopX:       make(chan error, 1000),
		process:     new(mutexRunner),
		Wait:        make(chan struct{}),
		kill:        make(chan struct{}),
	}
}

type mutexRunner struct {
	mu sync.Mutex
	x  int
}

// popRun Будем отмечать количество отключенных рутин
func (c *mutexRunner) popRun() (i int) {
	c.mu.Lock()
	c.x--
	i = c.x
	c.mu.Unlock()
	return
}

// addRun Ведем подсчет запущенных рутин
func (c *mutexRunner) addRun() (i int) {
	c.mu.Lock()
	c.x++
	i = c.x
	c.mu.Unlock()
	return
}
func (c *mutexRunner) getCores() (cores int) {
	c.mu.Lock()
	cores = c.x
	c.mu.Unlock()
	return
}

func (m *Conveer) GetCores() (cores int) {
	cores = m.process.getCores()
	return
}

func (m *Conveer) RunMinions(parallels int) {
	for i := 1; i <= parallels; i++ {
		go m.runMinion(m.process.addRun())
	}
	return
}

func (m *Conveer) runMinion(gopher int) {
	m.circle(gopher)
}

const errTextKilling = "Прервано - узел:%s, рутина:%d, очередь:%d, %s\n"

func (m *Conveer) circle(gopher int) {
	resultChan := make(chan struct{})
	for {
		select {
		case element := <-m.InputChan:
			go func() {
				m.ProcessFunc(gopher, element, m.Inputs, m.Outputs)
				resultChan <- struct{}{}
			}()
			// Исполнение функции может зависнуть, что помешает завершению
			select {
			case <-resultChan:
			case <-m.kill:
				m.process.popRun()
				m.stopX <- fmt.Errorf(errTextKilling, m.Name, gopher, len(m.InputChan), "горутина оставлена сборщику")
				return
			}
		case <-m.terminate:
			if len(m.InputChan) == 0 { // Корректно завершим оставшиеся задания
				m.process.popRun()
				m.stopX <- nil
				return
			}
		case <-m.kill:
			m.process.popRun()
			m.stopX <- fmt.Errorf(errTextKilling, m.Name, gopher, len(m.InputChan), "исполнение завершено")
			return
		}
	}
}
