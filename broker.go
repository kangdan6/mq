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
	"context"
	"errors"
	"sync"
)

type Broker struct {
	mutex sync.RWMutex
	// key是topic
	topics map[string][]chan Msg
	// 各个主题 capacity一致
	capacity int
}

// NewBroker 创建基于内存的消息队列
// topics注册的主题，必须提前注册
// capacity 容量 必须大于0
func NewBroker(topics []string, capacity int) (*Broker, error) {
	if capacity <= 0 {
		return nil, errors.New("capacity 错误")
	}
	b := &Broker{
		capacity: capacity,
	}
	b.topics = make(map[string][]chan Msg)
	for _, topic := range topics {
		b.topics[topic] = make([]chan Msg, 0)
	}
	return b, nil
}

// Topics 获取注册的主题
func (b *Broker) Topics() ([]string, error) {
	// 加读锁 防止其他人写
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	res := []string{}
	for k := range b.topics {
		res = append(res, k)
	}
	return res, nil
}

// Send Msg不使用指针是为了预防一个goroutine修改了数据，其他goroutine收到的数据就都修改了
func (b *Broker) Send(ctx context.Context, topic string, m Msg) error {
	// 加读锁 防止其他人写
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	// 如果topic不存在则报错
	if _, ok := b.topics[topic]; !ok {
		return errors.New("topic 错误")
	}
	chans := b.topics[topic]
	for _, ch := range chans {
		select {
		case ch <- m:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Subscribe 由使用者指定topic 只能消费订阅后的发布的最新消息，之前的老消息无法消费
func (b *Broker) Subscribe(topic string) (<-chan Msg, error) {
	// 加读锁 防止其他人写
	b.mutex.RLock()
	// 如果topic不存在则报错
	if _, ok := b.topics[topic]; !ok {
		return nil, errors.New("topic 错误")
	}
	b.mutex.RUnlock()

	res := make(chan Msg, b.capacity)
	// double-check
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.topics[topic] = append(b.topics[topic], res)
	return res, nil
}

// Close 关闭消息队列，不影响消费者消费剩余消息
func (b *Broker) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for topic, chans := range b.topics {
		b.topics[topic] = []chan Msg{}
		for _, ch := range chans {
			close(ch)
		}
	}
	return nil
}
