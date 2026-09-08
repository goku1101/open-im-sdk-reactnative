//go:build integration

package syncv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache/cachekey"
	storageRedis "github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache/redis"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database/mgo"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/openimsdk/open-im-server/v3/pkg/notificationboundary"
	"github.com/openimsdk/open-im-server/v3/pkg/syncv2/pb"
	"github.com/openimsdk/open-im-server/v3/pkg/util/conversationutil"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/mw"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func rpcCode(err error) int {
	var e errs.CodeError
	if errors.As(err, &e) {
		return e.Code()
	}
	return -1
}

// Explicit opt-in only. Use disposable, dedicated local dependencies, never a
// production Redis: OpenIM's allocator/read cache keys have no DB namespace.
func TestRealMongoRedisGRPC(t *testing.T) {
	if err := log.SetTemporaryLevel(log.LevelError, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	uri, addr := os.Getenv("SYNCV2_TEST_MONGO_URI"), os.Getenv("SYNCV2_TEST_REDIS_ADDR")
	if uri == "" || addr == "" {
		t.Skip("set SYNCV2_TEST_MONGO_URI and SYNCV2_TEST_REDIS_ADDR to disposable local containers")
	}
	if !strings.HasPrefix(uri, "mongodb://127.0.0.1:") || !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Fatal("only disposable localhost dependencies allowed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var finds atomic.Int64
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetMonitor(&event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "find" {
			finds.Add(1)
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatal(err)
	}
	db := client.Database("syncv2_test_" + primitive.NewObjectID().Hex())
	t.Cleanup(func() {
		if err := db.Drop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := mgo.NewConversationMongo(db); err != nil {
		t.Fatal(err)
	}
	seqDB, err := mgo.NewSeqConversationMongo(db)
	if err != nil {
		t.Fatal(err)
	}
	userDB, err := mgo.NewSeqUserMongo(db)
	if err != nil {
		t.Fatal(err)
	}
	msgDB, err := mgo.NewMsgMongo(db)
	if err != nil {
		t.Fatal(err)
	}
	reader := controller.NewCommonMsgDatabase(msgDB, storageRedis.NewMsgCache(rdb, msgDB), storageRedis.NewSeqUserCacheRedis(rdb, userDB), storageRedis.NewSeqConversationCacheRedis(rdb, seqDB), nil)
	backend := &MongoBackend{DB: db, Redis: rdb}
	store := &RedisSnapshots{Client: rdb}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(mw.GrpcServer())
	msgService := &MessageService{Backend: backend, Reader: reader}
	pb.RegisterMessageSyncServer(server, msgService)
	conn, err := grpc.NewClient("passthrough:///syncv2-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), mw.GrpcClient())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); server.Stop(); listener.Close() })
	messages := pb.NewMessageSyncClient(conn)
	settings := DefaultSettings()
	pb.RegisterConversationSyncServer(server, &ConversationService{Settings: settings, Backend: backend, Store: store, Messages: messages})
	go server.Serve(listener)
	conversations := pb.NewConversationSyncClient(conn)
	user := "v2_" + primitive.NewObjectID().Hex()
	ctx = mcontext.WithOpUserIDContext(ctx, user)
	ctx = context.WithValue(ctx, constant.OperationID, "syncv2-integration")
	const count = 50000
	ids := make([]string, count)
	old := time.Now().Add(-31 * 24 * time.Hour).UnixMilli()
	for start := 0; start < count; start += 500 {
		rows := make([]any, 0, 500)
		seqRows := make([]any, 0, 500)
		users := make([]any, 0, 500)
		docs := make([]any, 0, 500)
		for i := start; i < start+500; i++ {
			id := fmt.Sprintf("si_%s_%05d", user, i)
			ids[i] = id
			read := int64(10)
			when := old
			if i%1000 == 1 {
				read = 9
			}
			if i%1000 == 2 {
				when = time.Now().UnixMilli()
			}
			rows = append(rows, model.Conversation{OwnerUserID: user, ConversationID: id, IsPinned: i%1000 == 0})
			seqRows = append(seqRows, model.SeqConversation{ConversationID: id, MaxSeq: 10})
			users = append(users, model.SeqUser{UserID: user, ConversationID: id, ReadSeq: read})
			msgs := make([]*model.MsgInfoModel, 10)
			msgs[9] = &model.MsgInfoModel{Msg: &model.MsgDataModel{Seq: 10, SendTime: when, SendID: user, ClientMsgID: id, Content: "fixture"}}
			docs = append(docs, model.MsgDocModel{DocID: id + ":0", Msg: msgs})
		}
		for name, values := range map[string][]any{database.ConversationName: rows, database.SeqConversationName: seqRows, database.SeqUserName: users, model.MsgTableName: docs} {
			if _, err := db.Collection(name).InsertMany(ctx, values); err != nil {
				t.Fatal(err)
			}
		}
	}
	baselineID := primitive.NewObjectID()
	if _, err := db.Collection(database.ConversationVersionName).InsertOne(ctx, model.VersionLogTable{ID: baselineID, DID: user, Version: 7}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []bson.D{
		{{Key: "find", Value: database.ConversationName}, {Key: "filter", Value: bson.M{"owner_user_id": user}}, {Key: "projection", Value: bson.M{"conversation_id": 1, "_id": 0}}, {Key: "sort", Value: bson.D{{Key: "conversation_id", Value: 1}}}},
		{{Key: "find", Value: database.SeqUserName}, {Key: "filter", Value: bson.M{"user_id": user, "conversation_id": bson.M{"$in": ids[:500]}}}},
		{{Key: "find", Value: model.MsgTableName}, {Key: "filter", Value: bson.M{"doc_id": bson.M{"$in": []string{ids[0] + ":0", ids[1] + ":0"}}}}},
	} {
		var plan bson.M
		if err := db.RunCommand(ctx, bson.D{{Key: "explain", Value: query}, {Key: "verbosity", Value: "queryPlanner"}}).Decode(&plan); err != nil {
			t.Fatal(err)
		}
		encoded, err := bson.MarshalExtJSON(plan, false, false)
		if err != nil || !strings.Contains(string(encoded), "IXSCAN") || strings.Contains(string(encoded), "COLLSCAN") {
			t.Fatal("query not indexed", string(encoded), err)
		}
	}
	finds.Store(0)
	began := time.Now()
	cold, err := conversations.Snapshot(ctx, &pb.SnapshotRequest{UserID: user, PageSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	coldTime, coldFinds := time.Since(began), finds.Load()
	if cold.Total != 150 || cold.Mode != "filtered" || cold.Version != 7 || cold.VersionID != baselineID.Hex() {
		t.Fatal(cold)
	}
	if coldFinds > 410 {
		t.Fatalf("cold query fanout: %d finds", coldFinds)
	}
	t.Cleanup(func() { _ = store.Release(context.Background(), user, cold.SnapshotID) })
	all := append([]string{}, cold.ConversationIDs...)
	cursor := cold.Cursor
	for cursor != "" {
		p, err := conversations.Snapshot(ctx, &pb.SnapshotRequest{UserID: user, SnapshotID: cold.SnapshotID, Cursor: cursor, PageSize: 50})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, p.ConversationIDs...)
		cursor = p.Cursor
	}
	if len(all) != 150 {
		t.Fatal(len(all))
	}
	// Notifications page the entire parent set, not the selected chat IDs.
	n, err := conversations.Snapshot(ctx, &pb.SnapshotRequest{UserID: user, SnapshotID: cold.SnapshotID, Scope: "notifications"})
	if err != nil || n.Total != count || !n.IncludeSelf || len(n.ConversationIDs) != 500 {
		t.Fatal(n, err)
	}
	seenNotifications := len(n.ConversationIDs)
	for n.HasMore {
		n, err = conversations.Snapshot(ctx, &pb.SnapshotRequest{UserID: user, SnapshotID: cold.SnapshotID, Scope: "notifications", Cursor: n.Cursor})
		if err != nil || n.IncludeSelf {
			t.Fatal(n, err)
		}
		seenNotifications += len(n.ConversationIDs)
	}
	if seenNotifications != count {
		t.Fatal(seenNotifications)
	}
	for start := 0; start < count; start += 500 {
		pipe := rdb.Pipeline()
		for _, id := range ids[start : start+500] {
			pipe.HSet(ctx, cachekey.GetMallocSeqKey(id), "CURR", 10, "LAST", 10, "TIME", time.Now().UnixMilli())
			pipe.Expire(ctx, cachekey.GetMallocSeqKey(id), 5*time.Minute)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	finds.Store(0)
	began = time.Now()
	warm, err := conversations.Snapshot(ctx, &pb.SnapshotRequest{UserID: user})
	if err != nil {
		t.Fatal(err)
	}
	warmTime, warmFinds := time.Since(began), finds.Load()
	t.Cleanup(func() { _ = store.Release(context.Background(), user, warm.SnapshotID) })
	if warm.Total != cold.Total || warmFinds > 410 {
		t.Fatal(warm.Total, warmFinds)
	}
	_, err = conversations.Snapshot(ctx, &pb.SnapshotRequest{UserID: user})
	if rpcCode(err) != int(codes.ResourceExhausted) {
		t.Fatalf("quota error lost through middleware: %v", err)
	}
	other := mcontext.WithOpUserIDContext(ctx, "other")
	if _, err = conversations.Snapshot(other, &pb.SnapshotRequest{UserID: "other", SnapshotID: cold.SnapshotID}); rpcCode(err) != int(codes.PermissionDenied) {
		t.Fatal("owner binding", err)
	}
	if _, err = messages.States(other, &pb.StateRequest{UserID: user, ConversationIDs: ids[:1]}); err == nil {
		t.Fatal("cross-user read allowed")
	}
	empty, err := messages.States(ctx, &pb.StateRequest{UserID: user})
	if err != nil || len(empty.States) != 0 {
		t.Fatal(empty, err)
	}
	state, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: ids[:1], IncludeDetails: true})
	if err != nil || len(state.States) != 1 || state.States[0].Conversation == nil {
		t.Fatal(state, err)
	}
	pull, err := messages.Pull(ctx, &pb.PullRequest{UserID: user, ConversationID: ids[0], Seqs: []int64{10, 11}, StateToken: state.States[0].StateToken})
	if err != nil || len(pull.Messages) != 1 || pull.Messages[0].Seq != 10 {
		t.Fatal(pull, err)
	}
	if _, err := db.Collection(database.SeqUserName).UpdateOne(ctx, bson.M{"user_id": user, "conversation_id": ids[0]}, bson.M{"$set": bson.M{"min_seq": 11, "read_seq": 1}}); err != nil {
		t.Fatal(err)
	}
	_, err = messages.Pull(ctx, &pb.PullRequest{UserID: user, ConversationID: ids[0], Seqs: []int64{10}, StateToken: state.States[0].StateToken})
	if rpcCode(err) != int(codes.Aborted) {
		t.Fatal("clear should invalidate", err)
	}
	cleared, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: ids[:1]})
	if err != nil || cleared.States[0].UnreadCount != 0 {
		t.Fatal(cleared, err)
	}
	if _, err := db.Collection(database.ConversationName).DeleteOne(ctx, bson.M{"owner_user_id": user, "conversation_id": ids[0]}); err != nil {
		t.Fatal(err)
	}
	missing, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: ids[:1]})
	if err != nil || len(missing.States) != 0 || len(missing.UnavailableIDs) != 1 {
		t.Fatal(missing, err)
	}
	// Changing ownership never changes the fixed snapshot collection.
	again, err := conversations.Snapshot(ctx, &pb.SnapshotRequest{UserID: user, SnapshotID: cold.SnapshotID, PageSize: 50})
	if err != nil || again.Total != 150 || again.ConversationIDs[0] != ids[0] {
		t.Fatal(again, err)
	}
	// Reserved Mongo tail and missing data are unknown, not "empty" or "old".
	if err := rdb.Del(ctx, cachekey.GetMallocSeqKey(ids[3])).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection(database.SeqConversationName).UpdateOne(ctx, bson.M{"conversation_id": ids[3]}, bson.M{"$set": bson.M{"max_seq": 100}}); err != nil {
		t.Fatal(err)
	}
	unknown, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: ids[3:4]})
	if err != nil || unknown.States[0].Known || !Keep(unknown.States[0], time.Now()) {
		t.Fatal(unknown, err)
	}
	gap, err := messages.Pull(ctx, &pb.PullRequest{UserID: user, ConversationID: ids[3], Seqs: []int64{50}, StateToken: unknown.States[0].StateToken})
	if err != nil || len(gap.Messages) != 0 || len(gap.UnresolvedSeqs) != 1 || gap.UnresolvedSeqs[0] != 50 {
		t.Fatal("missing entry presented as deletion", gap, err)
	}
	// Self notification remains accessible even with an empty parent set.
	self, err := messages.States(ctx, &pb.StateRequest{UserID: user, Notifications: true, IncludeSelf: true})
	if err != nil || len(self.States) != 1 || self.States[0].ConversationID != conversationutil.GetSelfNotificationConversationID(user) {
		t.Fatal(self, err)
	}
	selfID := self.States[0].ConversationID
	if _, err := db.Collection(database.SeqConversationName).InsertOne(ctx, model.SeqConversation{ConversationID: selfID, MaxSeq: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection(model.MsgTableName).InsertOne(ctx, model.MsgDocModel{DocID: selfID + ":0", Msg: []*model.MsgInfoModel{{Msg: &model.MsgDataModel{Seq: 1, SendTime: time.Now().UnixMilli(), Content: "notification fixture"}}}}); err != nil {
		t.Fatal(err)
	}
	self, err = messages.States(ctx, &pb.StateRequest{UserID: user, Notifications: true, IncludeSelf: true})
	if err != nil {
		t.Fatal(err)
	}
	selfPull, err := messages.Pull(ctx, &pb.PullRequest{UserID: user, Notifications: true, Self: true, Seqs: []int64{1}, StateToken: self.States[0].StateToken})
	if err != nil || len(selfPull.Messages) != 1 {
		t.Fatal(selfPull, err)
	}
	// The shared single-value/batch read cache encoding is an integer in `value`.
	if err := rdb.HSet(ctx, cachekey.GetSeqUserReadSeqKey(ids[1], user), "value", "10").Err(); err != nil {
		t.Fatal(err)
	}
	readState, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: ids[1:2]})
	if err != nil || readState.States[0].HasReadSeq != 10 || readState.States[0].UnreadCount != 0 {
		t.Fatal(readState, err)
	}
	// Group visibility: a former member's historical cap wins over newer group
	// messages; zero caps during quit cannot turn into unlimited access.
	groupID := "group_" + user
	groupConv := "sg_" + groupID
	for name, doc := range map[string]any{
		database.GroupName:           model.Group{GroupID: groupID},
		database.ConversationName:    model.Conversation{OwnerUserID: user, ConversationID: groupConv, GroupID: groupID, IsPinned: true},
		database.SeqConversationName: model.SeqConversation{ConversationID: groupConv, MaxSeq: 20},
		database.SeqUserName:         model.SeqUser{ConversationID: groupConv, UserID: user, ReadSeq: 10},
		model.MsgTableName:           model.MsgDocModel{DocID: groupConv + ":0", Msg: []*model.MsgInfoModel{{Msg: &model.MsgDataModel{Seq: 10, SendTime: old}}, {Msg: &model.MsgDataModel{Seq: 20, SendTime: time.Now().UnixMilli()}}}},
	} {
		if _, err := db.Collection(name).InsertOne(ctx, doc); err != nil {
			t.Fatal(err)
		}
	}
	zeroCap, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: []string{groupConv}})
	if err != nil || len(zeroCap.States) != 0 {
		t.Fatal("pinned quit window exposed", zeroCap, err)
	}
	for _, name := range []string{database.ConversationName, database.SeqUserName} {
		if _, err := db.Collection(name).UpdateOne(ctx, bson.M{"conversation_id": groupConv}, bson.M{"$set": bson.M{"max_seq": 10}}); err != nil {
			t.Fatal(err)
		}
	}
	frozen, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: []string{groupConv}})
	if err != nil || len(frozen.States) != 1 || frozen.States[0].MaxSeq != 10 || frozen.States[0].LastMessageTime != old || frozen.States[0].UnreadCount != 0 {
		t.Fatal(frozen, err)
	}
	groupNotifications, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: []string{groupConv}, Notifications: true})
	if err != nil || len(groupNotifications.States) != 0 {
		t.Fatal("former member notification stream exposed", groupNotifications, err)
	}
	if _, err := db.Collection(database.GroupMemberName).InsertOne(ctx, model.GroupMember{GroupID: groupID, UserID: user}); err != nil {
		t.Fatal(err)
	}
	activeNotifications, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: []string{groupConv}, Notifications: true})
	if err != nil || len(activeNotifications.States) != 1 {
		t.Fatal(activeNotifications, err)
	}
	for _, kind := range []int32{constant.MemberQuitNotification, constant.MemberKickedNotification, constant.GroupDismissedNotification} {
		gid := fmt.Sprintf("%s_terminal_%d", user, kind)
		parent := "sg_" + gid
		stream := "n_" + gid
		var tips any
		switch kind {
		case constant.MemberQuitNotification:
			tips = &sdkws.MemberQuitTips{QuitUser: &sdkws.GroupMemberFullInfo{UserID: user}}
		case constant.MemberKickedNotification:
			tips = &sdkws.MemberKickedTips{KickedUserList: []*sdkws.GroupMemberFullInfo{{UserID: user}}}
		case constant.GroupDismissedNotification:
			tips = &sdkws.GroupDismissedTips{Group: &sdkws.GroupInfo{GroupID: gid}}
		}
		detail, _ := json.Marshal(tips)
		content, _ := json.Marshal(&sdkws.NotificationElem{Detail: string(detail)})
		terminal := &sdkws.MsgData{GroupID: gid, Seq: 7, SessionType: constant.ReadGroupChatType, MsgFrom: constant.SysMsgType, ContentType: kind, Content: content, ClientMsgID: "terminal", SendTime: time.Now().UnixMilli()}
		entries := make([]*model.MsgInfoModel, 9)
		entries[6] = &model.MsgInfoModel{Msg: &model.MsgDataModel{Seq: 7, ContentType: kind, Content: string(content), ClientMsgID: "terminal", SendTime: terminal.SendTime}}
		entries[7] = &model.MsgInfoModel{Msg: &model.MsgDataModel{Seq: 8, Content: "must not be visible after departure", SendTime: terminal.SendTime + 1}}
		for name, doc := range map[string]any{database.GroupName: model.Group{GroupID: gid}, database.GroupMemberName: model.GroupMember{GroupID: gid, UserID: user}, database.ConversationName: model.Conversation{OwnerUserID: user, ConversationID: parent, GroupID: gid}, database.SeqConversationName: model.SeqConversation{ConversationID: stream, MaxSeq: 9}, model.MsgTableName: model.MsgDocModel{DocID: stream + ":0", Msg: entries}} {
			if _, err := db.Collection(name).InsertOne(ctx, doc); err != nil {
				t.Fatal(err)
			}
		}
		store := &notificationboundary.Store{DB: db}
		if err := store.Record(ctx, stream, []*sdkws.MsgData{terminal}); err != nil {
			t.Fatal(err)
		}
		// A replay cannot lower a newer grant.
		terminal.Seq = 6
		if err := store.Record(ctx, stream, []*sdkws.MsgData{terminal}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Collection(database.GroupMemberName).DeleteMany(ctx, bson.M{"group_id": gid}); err != nil {
			t.Fatal(err)
		}
		if kind == constant.GroupDismissedNotification {
			if _, err := db.Collection(database.GroupName).UpdateOne(ctx, bson.M{"group_id": gid}, bson.M{"$set": bson.M{"status": constant.GroupStatusDismissed}}); err != nil {
				t.Fatal(err)
			}
		}
		state, err := messages.States(ctx, &pb.StateRequest{UserID: user, ConversationIDs: []string{parent}, Notifications: true})
		if err != nil || len(state.States) != 1 || state.States[0].MaxSeq != 7 || state.States[0].PermissionMaxSeq != 7 {
			t.Fatal(kind, state, err)
		}
		pulled, err := messages.Pull(ctx, &pb.PullRequest{UserID: user, ConversationID: parent, Notifications: true, Seqs: []int64{7, 8}, StateToken: state.States[0].StateToken})
		if err != nil || len(pulled.Messages) != 1 || pulled.Messages[0].ContentType != kind {
			t.Fatal(kind, pulled, err)
		}
		// Owning an old conversation does not grant a non-recipient access.
		if _, err := db.Collection(database.ConversationName).InsertOne(ctx, model.Conversation{OwnerUserID: "other", ConversationID: parent, GroupID: gid}); err != nil {
			t.Fatal(err)
		}
		denied, err := messages.States(other, &pb.StateRequest{UserID: "other", ConversationIDs: []string{parent}, Notifications: true})
		if err != nil || len(denied.States) != 0 {
			t.Fatal("notification audience bypass", denied, err)
		}
	}
	// Atomic reservation under concurrent logins (not a per-process map).
	quotaSettings := DefaultSettings()
	quotaSettings.MaxSnapshotsPerUser = 1
	quotaUser := "quota_" + user
	var wg sync.WaitGroup
	var accepted atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := store.Reserve(ctx, quotaUser, quotaSettings)
			if err == nil {
				accepted.Add(1)
				t.Cleanup(func() { _ = store.Release(context.Background(), quotaUser, id) })
			} else if e, ok := err.(interface{ Code() int }); !ok || e.Code() != int(codes.ResourceExhausted) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatal("non-atomic quota", accepted.Load())
	}
	// Force expiry without sleeping or changing the Redis server clock.
	if err := rdb.Del(ctx, snapshotKeys(user, warm.SnapshotID)[2]).Err(); err != nil {
		t.Fatal(err)
	}
	_, err = conversations.Snapshot(ctx, &pb.SnapshotRequest{UserID: user, SnapshotID: warm.SnapshotID})
	if rpcCode(err) != int(codes.FailedPrecondition) {
		t.Fatal("expiry code lost", err)
	}
	// Release is idempotent and frees quota immediately, not after the TTL.
	for i := 0; i < 2; i++ {
		if _, err := conversations.Release(ctx, &pb.ReleaseRequest{UserID: user, SnapshotID: warm.SnapshotID}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conversations.Release(other, &pb.ReleaseRequest{UserID: "other", SnapshotID: cold.SnapshotID}); rpcCode(err) != int(codes.PermissionDenied) {
		t.Fatal("foreign release", err)
	}
	id, err := store.Reserve(ctx, user, settings)
	if err != nil {
		t.Fatal("release failed to free quota", err)
	}
	if err := store.Release(ctx, user, id); err != nil {
		t.Fatal(err)
	}
	t.Logf("50000 conversations: cold=%s (%d Mongo finds), warm=%s (%d Mongo finds), selected=150, notification parents=50000", coldTime, coldFinds, warmTime, warmFinds)
}
