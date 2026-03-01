package main

import (
	"fmt" // 替换 ioutil
	"log"
	"os"
	"plugin"

	"6.5840/mr"
)

func main() {
	if len(os.Args) != 3 {
		// 建议改为明确的输入提示
		fmt.Fprintf(os.Stderr, "Usage: go run test.go xxx.so input.txt\n")
		os.Exit(1)
	}

	mapf := loadPlugin(os.Args[1])
	inputFileName := os.Args[2]

	// 推荐使用 os.ReadFile，更简洁，且会自动关闭文件
	content, err := os.ReadFile(inputFileName)
	if err != nil {
		log.Fatalf("无法读取文件 %v: %v", inputFileName, err)
	}

	// 执行 Map 函数
	kva := mapf(inputFileName, string(content))

	// 打印结果
	fmt.Printf("成功处理文件，产生 %d 条键值对。\n", len(kva))
	if len(kva) > 0 {
		fmt.Printf("样例输出: %+v\n", kva[0])
	}
}

func loadPlugin(filename string) func(string, string) []mr.KeyValue {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("无法加载插件 %v (确保已执行 go build -buildmode=plugin)", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("在插件中找不到 Map 函数")
	}

	// 安全检查类型断言
	mapf, ok := xmapf.(func(string, string) []mr.KeyValue)
	if !ok {
		log.Fatalf("Map 函数签名不正确")
	}
	return mapf
}
