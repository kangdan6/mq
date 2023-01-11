// Copyright 2021 gotomicro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mq

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMq_Send(t *testing.T) {
	b := &mq{}

	// 模拟发送者
	go func() {
		for {
			err := b.Send(Msg{content: time.Now().String()})
			if err != nil {
				t.Log(err)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// 关闭chan
	go func() {
		time.Sleep(time.Millisecond * 30)
		b.Close()
	}()

	// 模拟消费者
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("消费者 %d", i)
		go func() {
			defer wg.Done()
			msgs, err := b.Subscribe(100)
			if err != nil {
				t.Log(err)
				return
			}
			for msg := range msgs {
				fmt.Println(name, msg)
			}
		}()
	}
	wg.Wait()

}
