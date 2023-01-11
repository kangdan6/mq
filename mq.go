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
	"errors"
	"sync"
)

type Msg struct {
	content string
}

type mq struct {
	mutex sync.RWMutex
	chans []chan Msg
}

// Send Msg不使用指针是为了预防一个goroutine修改了数据，其他goroutine收到的数据就都修改了
func (b *mq) Send(m Msg) error {
	// 加读锁视为了避免其他人写
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	for _, ch := range b.chans {
		select {
		case ch <- m:
		default:
			return errors.New("消息队列已满")
		}
	}
	return nil
}

// Subscribe 由使用者指定capacity
func (b *mq) Subscribe(capacity int) (<-chan Msg, error) {
	res := make(chan Msg, capacity)
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.chans = append(b.chans, res)
	return res, nil
}

func (b *mq) Close() error {
	b.mutex.Lock()
	chans := b.chans
	// 避免了重复 close chan 的问题
	b.chans = nil
	b.mutex.Unlock()

	for _, ch := range chans {
		close(ch)
	}
	return nil
}
