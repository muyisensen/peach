package art

import (
	"reflect"
	"testing"
)

func Test_isNil(t *testing.T) {
	var node *node4
	var other = &node4{}
	type args struct {
		no treeNode
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nil",
			args: args{
				no: nil,
			},
			want: true,
		},
		{
			name: "nil struct pointer",
			args: args{
				no: node,
			},
			want: true,
		},
		{
			name: "not nil",
			args: args{
				no: other,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNil(tt.args.no); got != tt.want {
				t.Errorf("isNil() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_minimum(t *testing.T) {
	type args struct {
		a int
		b int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "0 > -1",
			args: args{
				a: 0,
				b: -1,
			},
			want: -1,
		},
		{
			name: "-1 < 0",
			args: args{
				a: -1,
				b: 0,
			},
			want: -1,
		},
		{
			name: " 0 == 0",
			args: args{
				a: 0,
				b: 0,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := minimum(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("minimum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_largeCommonPerfix(t *testing.T) {
	type args struct {
		a []byte
		b []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "case 1",
			args: args{
				a: []byte("abc"),
				b: []byte("abcd"),
			},
			want: []byte("abc"),
		},
		{
			name: "case 2",
			args: args{
				a: []byte("bc"),
				b: []byte("abcd"),
			},
			want: []byte{},
		},
		{
			name: "case 3",
			args: args{
				a: []byte{},
				b: []byte("abcd"),
			},
			want: []byte{},
		},
		{
			name: "case 4",
			args: args{
				a: nil,
				b: nil,
			},
			want: []byte{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := largeCommonPerfix(tt.args.a, tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("largeCommonPerfix() = %v, want %v", got, tt.want)
			}
		})
	}
}
