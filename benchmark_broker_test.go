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
	"testing"
	"time"
)

// BenchmarkBroker_send
// go test -benchmem -run=^$ -tags e2e -bench ^BenchmarkBroker_send$ mq
// goos: darwin
// goarch: arm64
// pkg: mq
// BenchmarkBroker_send-8   	   56708	     21604 ns/op	    8932 B/op	     194 allocs/op
// PASS
// ok  	mq	1.695s
func BenchmarkBroker_send(b *testing.B) {
	testCases := []struct {
		name          string
		capacity      int
		topics        []string
		consumerTopic string
		timeout       time.Duration
		sendtimes     int

		wantErr error
	}{
		{
			name:          "normal send",
			capacity:      100,
			timeout:       time.Microsecond,
			topics:        []string{"topic1"},
			consumerTopic: "topic1",
			sendtimes:     10,
		},
		{
			name:          "send full",
			capacity:      10,
			topics:        []string{"topic1"},
			consumerTopic: "topic1",
			timeout:       time.Millisecond,
			sendtimes:     20,

			wantErr: context.DeadlineExceeded,
		},
		{
			name:          "send error topic",
			capacity:      10,
			topics:        []string{"topic1"},
			consumerTopic: "topic-error",

			timeout:   time.Millisecond,
			sendtimes: 20,

			wantErr: errors.New("topic 错误"),
		},
	}
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			b, _ := NewBroker(tc.capacity, tc.topics...)
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			// 发送 sendtimes 次消息
			for i := 0; i < tc.sendtimes; i++ {
				b.Send(ctx, tc.consumerTopic, Msg{content: time.Now().String()})

			}
		}
	}

}

// BenchmarkBroker_subscribe
// go test -benchmem -run=^$ -tags e2e -bench ^BenchmarkBroker_subscribe$ mq
// goos: darwin
// goarch: arm64
// pkg: mq
// BenchmarkBroker_subscribe
// BenchmarkBroker_subscribe-8         7646            168684 ns/op          520959 B/op        638 allocs/op
// PASS
// ok      mq      3.292s
func BenchmarkBroker_subscribe(b *testing.B) {
	testCases := []struct {
		name          string
		capacity      int
		topics        []string
		consumerTopic string
		timeout       time.Duration
		sendtimes     int

		wantErr error
	}{
		{
			name:          "normal subscribe",
			capacity:      100,
			timeout:       time.Microsecond,
			topics:        []string{"topic1"},
			consumerTopic: "topic1",
			sendtimes:     1,
		},
		{
			name:          "subscribe error topic",
			capacity:      100,
			timeout:       time.Microsecond,
			topics:        []string{"topic1"},
			consumerTopic: "topic-error",
			sendtimes:     1,
		},
		{
			name:          "subscribe timeout",
			capacity:      10000,
			timeout:       -time.Microsecond,
			topics:        []string{"topic1"},
			consumerTopic: "topic1",
			sendtimes:     100,
			wantErr:       context.DeadlineExceeded,
		},
	}

	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			b, _ := NewBroker(10000, tc.topics...)

			go func() {
				msgs, _ := b.Subscribe(tc.consumerTopic)
				// 消费消息
				for range msgs {
					// 进行业务处理
					// fmt.Println(msg)
				}
			}()
			// 发送 sendtimes 次消息
			for i := 0; i < tc.sendtimes; i++ {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
					defer cancel()
					b.Send(ctx, tc.consumerTopic, Msg{content: time.Now().String()})
				}()
			}
		}

	}
}
