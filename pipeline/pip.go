package pipeline

import (
	"fmt"
	"sync"
)

// Зададим абстрактную модель для данных [4]interface{}
// 1[0]- данные any, 2[1]-ошибка error, 3[2]-завершено bool, 4[3] - Kill bool

// ProcessorFunc определяет логику обработки ноды.
// Она принимает карту входных каналов и карту выходных каналов.
// Функция должна управлять чтением из входов, записью в выходы и закрытием выходов.
type ProcessorFunc func(in map[string]chan [4]interface{}, out map[string]chan [4]interface{}, wg *sync.WaitGroup)

// Node представляет собой узел в графе обработки.
type Node struct {
	Name        string
	ProcessFunc ProcessorFunc
	Inputs      map[string]chan [4]interface{}
	Outputs     map[string]chan [4]interface{}
	wg          *sync.WaitGroup
}

// NewNode создает новый узел.
func NewNode(name string, procFunc ProcessorFunc, inputNames, outputNames []string) *Node {
	n := &Node{
		Name:        name,
		ProcessFunc: procFunc,
		Inputs:      make(map[string]chan [4]interface{}),
		Outputs:     make(map[string]chan [4]interface{}),
	}
	for _, name := range inputNames {
		n.Inputs[name] = make(chan [4]interface{}) // Инициализируем входной канал
	}
	for _, name := range outputNames {
		n.Outputs[name] = make(chan [4]interface{}) // Инициализируем выходной канал
	}
	return n
}

// Pipeline управляет выполнением графа.
type Pipeline struct {
	Nodes []*Node
	Wg    sync.WaitGroup
}

// NewPipeline создает новый пустой пайплайн.
func NewPipeline() *Pipeline {
	return &Pipeline{}
}

// AddNode добавляет узел в пайплайн.
func (p *Pipeline) AddNode(node *Node) {
	node.wg = &p.Wg
	p.Nodes = append(p.Nodes, node)
}

// Connect соединяет определенный выход fromNode с определенным входом toNode.
// Это критический шаг для построения графа.
func (p *Pipeline) Connect(fromNode, toNode *Node, fromPort, toPort string) error {
	output, ok := fromNode.Outputs[fromPort]
	if !ok {
		return fmt.Errorf("выходной порт '%s' ноды '%s' не найден", fromPort, fromNode.Name)
	}
	_, ok = toNode.Inputs[toPort]
	if !ok {
		return fmt.Errorf("входной порт '%s' ноды '%s' не найден", toPort, toNode.Name)
	}

	// Переназначаем входной канал целевой ноды на выходной канал исходной ноды.
	// Теперь обе ноды используют один и тот же канал для передачи данных.
	toNode.Inputs[toPort] = output
	// Примечание: исходный input канал toNode, созданный в NewNode, теперь утекает, но это управляемо.
	return nil
}

// Run запускает все ноды пайплайна в параллельных горутинах.
func (p *Pipeline) Run() {
	for _, node := range p.Nodes {
		p.Wg.Add(1)
		// Запускаем функцию обработки каждой ноды.
		// Функция сама отвечает за управление своими каналами и вызов p.Wg.Done()
		go node.ProcessFunc(node.Inputs, node.Outputs, &p.Wg)
	}
}

// Wait ожидает завершения всего пайплайна.
func (p *Pipeline) Wait() {
	p.Wg.Wait()
}
