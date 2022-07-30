package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

type StandAloneReader struct {
	txn *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{txn}
}

func (sr *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	if sr.txn == nil {
		return nil, IterNotValid
	}
	item, err := sr.txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	return item.Value()
}

func (sr *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return nil
}

func (sr *StandAloneReader) Close() {
	sr.txn.Discard()
	sr.txn = nil
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

var DBNotStart = errors.New("The db is not started")
var IterNotValid = errors.New("The iterator is not valid")

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db == nil {
		return DBNotStart
	}

	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.db == nil {
		return nil, DBNotStart
	}
	return NewStandAloneReader(s.db.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if s.db == nil {
		return DBNotStart
	}
	//wb := new(engine_util.WriteBatch)
	//for _, modify := range batch {
	//	cf := modify.Cf()
	//	key := modify.Key()
	//  val := modify.Value()
	//	if val != nil {
	//		// case put
	//		wb.SetCF(cf, key, val)
	//	} else {
	//		// case delete
	//		wb.DeleteCF(cf, key)
	//	}
	//}
	//err := wb.WriteToDB(s.db)
	txn := s.db.NewTransaction(true)
	for _, modify := range batch {
		cf := modify.Cf()
		key := modify.Key()
		val := modify.Value()
		if val != nil {
			// case put
			err := txn.Set(engine_util.KeyWithCF(cf, key), val)
			if err != nil {
				return err
			}
		} else {
			// case delete
			err := txn.Delete(engine_util.KeyWithCF(cf, key))
			if err != nil {
				return err
			}
		}
	}
	err := txn.Commit()
	return err
}
