package pipeline

const BUFFER_VALUE = 100

// Зададим абстрактную модель для данных [2]interface{}
// 1[0]- данные any, 2[1]-ошибка error, 3[2]-завершено bool, 4[3] - Kill bool

// ProcessorFunc определяет логику обработки ноды.
// Она принимает карту входных каналов и карту выходных каналов.
// Функция должна управлять чтением из входов, записью в выходы и закрытием выходов.
type ProcessorFunc func(gopher int, element [2]interface{}, inputs, outputs map[string]chan [2]interface{})

// Node представляет собой узел в графе обработки.
type Node struct {
	Name        string
	ProcessFunc ProcessorFunc
	Inputs      map[string]chan [2]interface{}
	Outputs     map[string]chan [2]interface{}
	Routines    int // Количество работающих рутин в ноде
	Conveers    *Conveer
}

// Pipeline управляет выполнением графа.
type Pipeline struct {
	nodes   []*Node
	Inputs  map[string]chan [2]interface{}
	Outputs map[string]chan [2]interface{}
}

// NewPipeline создает новый пустой пайплайн.
func NewPipeline() *Pipeline {
	p := &Pipeline{
		nodes:   make([]*Node, 0),
		Inputs:  make(map[string]chan [2]interface{}),
		Outputs: make(map[string]chan [2]interface{}),
	}
	return p
}

// AddNode NewNode создает новый узел. Добавляет узел в пайплайн.
func (p *Pipeline) AddNode(name string, procFunc ProcessorFunc, routines int) *Node {
	node := &Node{
		Name:        name,
		ProcessFunc: procFunc,
		Inputs:      p.Inputs,
		Outputs:     p.Outputs,
		Routines:    routines,
	}
	// AddNode добавляет узел в пайплайн.
	p.nodes = append(p.nodes, node)
	p.Inputs[name] = make(chan [2]interface{}, BUFFER_VALUE)
	p.Outputs[name] = make(chan [2]interface{}, BUFFER_VALUE)
	return node
}

// Run запускает все ноды пайплайна в параллельных горутинах.
func (p *Pipeline) Run() {
	for _, node := range p.nodes {
		// Запускаем горутины
		md5conveer := NewConveer(node, node.ProcessFunc, node.Inputs, node.Outputs)
		md5conveer.RunMinions(node.Routines)
		node.Conveers = md5conveer
	}
}

func (p *Pipeline) Stop() {
	for _, node := range p.nodes {
		// Останавливаем горутины
		node.Conveers.Stop()
	}
}

func (p *Pipeline) Kill() error {
	var errorsSlice []error
	for _, node := range p.nodes {
		// Бросаем горутины сборщику
		errRoutine := node.Conveers.Kill()
		if errRoutine != nil {
			errorsSlice = append(errorsSlice, errRoutine)
		}
	}
	return combineErrors(errorsSlice)
}
