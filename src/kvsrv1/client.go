package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// Get 用于获取键的当前值和版本。如果键不存在，则返回 ErrNoKey。对于其他所有错误，Get 会持续重试，直到成功为止。
//
// 你可以使用如下代码发送 RPC 调用：
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// args 和 reply 的类型（包括它们是否为指针）必须与 RPC 处理函数声明的参数类型匹配，并且 reply 必须作为指针传递。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{
		Key: key,
	}
	reply := rpc.GetReply{}
	for !ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply) {
	}
	// You will have to modify this function.
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if version is the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrNoVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have een processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// 仅当请求中的版本号与服务器端键的版本号匹配时，才使用值更新键。
// 如果版本号不匹配，服务器应返回 ErrNoVersion。
// 如果 Put 在第一次 RPC 调用时收到 ErrVersion，Put 应返回 ErrVersion，因为 Put 操作肯定未在服务器端执行。
// 如果服务器在重发 RPC 时返回 ErrVersion，则 Put 必须向应用程序返回 ErrMaybe，
// 因为之前的 RPC 可能已被服务器成功处理，但响应丢失，Clerk 无法确定 Put 操作是否已执行。
//
// 你可以使用如下代码发送 RPC 调用：
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// args 和 reply 的类型（包括它们是否为指针）必须与 RPC 处理函数声明的参数类型匹配，并且 reply 必须作为指针传递。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := rpc.PutReply{}
	for !ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply) {
	}

	return reply.Err
}
