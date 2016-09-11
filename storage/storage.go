package storage

import (
    "github.com/boltdb/bolt"
    "log"
    "encoding/json"
    "time"
    "fmt"
    "bytes"
    "errors"
    "strconv"
    "strings"
)

const ImageDataKey = "imageData"

const JobPrefix = "job_"
const TaskPrefix = "task_"
const ImgPrefix = "img_"
const GCSPrefix = "gcs_"
const ServerPrefix = "server_"
const FinishedPrefix = "done_"

const Done = "true"
const NotDone = "false"

type Store struct {
    db *bolt.DB
}

func NewStorage(db *bolt.DB) *Store {
    if db == nil {
        log.Fatalln("storage db is not defined")
    }
    return &Store{db:db}
}


//The new user ID is set on u once the data is persisted.
func (self *Store) NewTaskStorage(payloads  []*Payload) error {
    return self.db.Update(func(tx *bolt.Tx) error {
        job_name := fmt.Sprintf("%v%v",
            JobPrefix, time.Now().Format(time.RFC3339))

        job_b, err := tx.CreateBucketIfNotExists([]byte(job_name))
        if err != nil {
            return err
        }

        for _, task := range payloads {
            task_id, err := next_id(job_b)
            if self.IsErr(err) {
                return err
            }
            task_id = self.createTaskID(task_id)

            //0. create task bucket
            task_b, err := create_bucket(job_b, task_id)
            if self.IsErr(err) {
                return err
            }

            //2. Save Task info
            err = self.SaveTaskInfo(task_b, task)
            if self.IsErr(err) {
                return err
            }

            //3. Save Image Info
            err = self.SaveImageInfo(task_b, task)
            if self.IsErr(err) {
                return err
            }
        }

        return self.MarkNotDone(job_b)
    })
}

func (self *Store) MarkDone(b *bolt.Bucket) error {
    return b.Put([]byte("done"), []byte(Done))
}

func (self *Store) MarkNotDone(b *bolt.Bucket) error {
    return b.Put([]byte("done"), []byte(NotDone))
}

func (self *Store) IsDone(b *bolt.Bucket) bool {
    state := b.Get([]byte("done"))
    return string(state) == Done
}

func (self *Store) B(s string) []byte {
    return []byte(s)
}

func (self *Store) createTaskID(id interface{}) string {
    return fmt.Sprintf("%v%v", TaskPrefix, id)
}

func (self *Store) Put(b *bolt.Bucket, key string, value string) error {
    return b.Put(self.B(key), self.B(value))
}

func (self *Store) IsErr(err error) bool {
    return err != nil
}

//saves tasks info (includes creating imageData bucket on task)
func (self *Store) SaveTaskInfo(b *bolt.Bucket, task *Payload) error {
    var err error = nil
    // create imageData bucket on bucket
    _, err = create_bucket(b, ImageDataKey)
    if self.IsErr(err) {
        return err
    }

    var data = make(map[string]interface{})
    data[ "date"       ] = task.Date
    data[ "parcelId"   ] = task.ParcelId
    data[ "bucket"     ] = task.Bucket
    data[ "imageLevel" ] = task.ImageLevel
    data[ "imageType"  ] = task.ImageType

    for k, v := range data {
        val := fmt.Sprintf("%v", v)
        if err = self.Put(b, k, val); self.IsErr(err) {
            return err
        }
    }

    return self.MarkNotDone(b)
}

//saves images in a given task payload
func (self *Store) SaveImageInfo(b *bolt.Bucket, task *Payload) error {
    var err error = nil
    b, err = b.CreateBucketIfNotExists([]byte(ImageDataKey))
    if self.IsErr(err) {
        return err
    }

    imgdata := task.ImageData
    for _, img := range imgdata {
        k, err := next_id(b)
        if self.IsErr(err) {
            return err
        }
        //img key
        k = MakeImgKey(ImgPrefix, k)

        v, err := json.Marshal(img)
        if self.IsErr(err) {
            return err
        }

        if err = self.Put(b, k, string(v)); self.IsErr(err) {
            return err
        }
    }

    return nil
}

//returns all job buckets
func (self *Store) AllJobs() []string {
    keys := make([]string, 0)
    err := self.db.View(func(tx *bolt.Tx) error {
        c := tx.Cursor()
        prefix := []byte(JobPrefix)
        for k, v := c.Last(); k != nil; k, v = c.Prev() {
            if v == nil && bytes.HasPrefix(k, prefix) {
                //only buckets
                keys = append(keys, string(k))
            }
        }
        return nil
    })
    if err != nil {
        log.Fatalln(err)
    }
    return keys

}

//returns latest bucket not done
func (self *Store) LatestJob() string {
    var key string
    err := self.db.View(func(tx *bolt.Tx) error {
        c := tx.Cursor()
        prefix := []byte(JobPrefix)
        for k, v := c.Last(); k != nil; k, v = c.Prev() {
            if v == nil && bytes.HasPrefix(k, prefix) {
                b := tx.Bucket(k)
                if b != nil && !self.IsDone(b) {
                    key = string(k)
                    break
                }
            }
        }
        return nil
    })
    if err != nil {
        log.Fatalln(err)
    }
    return key
}

//returns all job buckets
func (self *Store) DeleteBucket(bucket string) bool {
    key := []byte(bucket)
    err := self.db.Update(func(tx *bolt.Tx) error {
        if tx.Bucket(key) != nil {
            return tx.DeleteBucket(key)
        }
        return nil
    })
    if err != nil {
        log.Fatalln(err)
    }
    return true
}

//concels job by marking it as done
func (self *Store) CancelJob(bucket string) bool {
    key := []byte(bucket)
    err := self.db.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket(key)
        if b != nil {
            return self.MarkDone(b)
        }
        return nil
    })
    if err != nil {
        log.Fatalln(err)
    }
    return true
}

//returns all job buckets
func (self *Store) GetGCSPayload(job string) []*UpObj {
    var path = job + "|"

    var gcs_payload = make([]*UpObj, 0)
    job_key := []byte(job)

    err := self.db.View(func(tx *bolt.Tx) error {

        jb := tx.Bucket(job_key)
        if jb == nil {
            PrintlnWarning("undefined job bucket...")
            return nil
        }

        tasks := self.GetAllTasksNotDone(jb)

        for _, tk := range tasks {
            tk_path := path + string(tk) + "|"
            tb := jb.Bucket(tk)
            imb := tb.Bucket([]byte(ImageDataKey))

            tsk_imgs, err := self.GetAllImgData(imb)
            if self.IsErr(err) {
                return err
            }
            for k, im := range tsk_imgs {
                im_path := tk_path + k
                o := &UpObj{
                    Src  : im.Src,
                    Dest : im.Dest,
                    Key  : im_path,
                    State: ImgPrefix,
                }
                gcs_payload = append(gcs_payload, o)
            }
        }
        return nil

    })

    if err != nil {
        log.Fatalln(err)
    }

    return gcs_payload
}

//returns all job buckets
func (self *Store) GetServerPayload(job string) []*UpObj {
    var path = job + "|"

    var gcs_payload = make([]*UpObj, 0)
    job_key := []byte(job)

    err := self.db.View(func(tx *bolt.Tx) error {

        jb := tx.Bucket(job_key)
        if jb == nil {
            PrintlnWarning("undefined job bucket...")
            return nil
        }

        tasks := self.GetAllTasksNotDone(jb)

        for _, tk := range tasks {
            tk_path := path + string(tk) + "|"
            tb := jb.Bucket(tk)
            imb := tb.Bucket([]byte(ImageDataKey))

            tsk_imgs, err := self.GetAllGCSData(imb)
            if self.IsErr(err) {
                return err
            }
            for k, im := range tsk_imgs {
                im_path := tk_path + k
                o := &UpObj{
                    Src  : im.Src,
                    Dest : im.Dest,
                    Key  : im_path,
                    State: GCSPrefix,
                }
                gcs_payload = append(gcs_payload, o)
            }
        }
        return nil

    })

    if err != nil {
        log.Fatalln(err)
    }

    return gcs_payload
}

func (self *Store) GetAllTasksNotDone(b *bolt.Bucket) [][]byte {
    var tasks = make([][]byte, 0)
    c := b.Cursor()
    for k, v := c.Seek([]byte(TaskPrefix)); bytes.HasPrefix(
        k, []byte(TaskPrefix)); k, v = c.Next() {
        if v == nil {
            tb := b.Bucket(k)
            if !self.IsDone(tb) {
                tasks = append(tasks, k)
            }
        }
    }
    return tasks
}

func (self *Store) GetAllImgData(b *bolt.Bucket) (map[string]Img, error) {
    var imgs = make(map[string]Img, 0)
    c := b.Cursor()
    for k, v := c.Seek([]byte(ImgPrefix)); bytes.HasPrefix(
        k, []byte(ImgPrefix)); k, v = c.Next() {
        var im = Img{}
        if err := json.Unmarshal(v, &im); err != nil {
            return imgs, err
        }
        imgs[string(k)] = im
    }
    return imgs, nil
}

func (self *Store) GetAllGCSData(b *bolt.Bucket) (map[string]Img, error) {
    var imgs = make(map[string]Img, 0)
    c := b.Cursor()
    for k, v := c.Seek([]byte(GCSPrefix)); bytes.HasPrefix(
        k, []byte(GCSPrefix)); k, v = c.Next() {
        var im = Img{}
        if err := json.Unmarshal(v, &im); err != nil {
            return imgs, err
        }
        imgs[string(k)] = im
    }
    return imgs, nil
}

func (self *Store) SetGCSImg(job_key, task_key, img_key string) bool {
    err := self.db.Update(func(tx *bolt.Tx) error {
        b, err := self.GetImgBucket(tx, job_key, task_key)
        if self.IsErr(err) {
            return err
        }
        k := img_key
        v := b.Get([]byte(k))
        if len(v) > 0 {
            err = b.Delete([]byte(k))
            if self.IsErr(err) {
                return err
            }
            k = MakeImgKey(GCSPrefix, ImgIDFromKey(k))
            return self.Put(b, k, string(v))
        }
        return nil
    })

    if err != nil {
        PrintlnError(err)
    }
    return err == nil
}

//update a list of keys
func (self *Store) SetGCSImgs(keys []string) bool {
    err := self.db.Update(func(tx *bolt.Tx) error {
        for _, key := range keys {
            tokens := strings.Split(key, "|")
            job_key, task_key, img_key := tokens[0], tokens[1], tokens[2]

            b, err := self.GetImgBucket(tx, job_key, task_key)
            if self.IsErr(err) {
                return err
            }

            k := img_key
            v := b.Get([]byte(k))
            if len(v) > 0 {
                err = b.Delete([]byte(k))
                if self.IsErr(err) {
                    return err
                }
                k = MakeImgKey(GCSPrefix, ImgIDFromKey(k))
                err := self.Put(b, k, string(v))
                if self.IsErr(err) {
                    return err
                }
            }
        }

        return nil
    })

    if err != nil {
        PrintlnError(err)
    }
    return err == nil
}

func (self *Store) SetServerImgs(job_key, task_key string, img_keys []string) bool {
    err := self.db.Update(func(tx *bolt.Tx) error {
        b, err := self.GetImgBucket(tx, job_key, task_key)
        if self.IsErr(err) {
            return err
        }
        for _, k := range img_keys {
            v := b.Get([]byte(k))
            if len(v) > 0 {

                err = b.Delete([]byte(k))

                if self.IsErr(err) {
                    PrintlnError(err)
                    return err
                }

                k := MakeImgKey(ServerPrefix, ImgIDFromKey(k))
                err := self.Put(b, k, string(v))
                if self.IsErr(err) {
                    return err
                }
            }
        }

        return nil
    })

    if err != nil {
        PrintlnError(err)
    }
    return err == nil
}

func (self *Store) GetServerTaskInfo(job_key, task_key string) (Payload, bool) {
    var taskinfo Payload
    err := self.db.View(func(tx *bolt.Tx) error {
        b, err := self.GetTaskBucket(tx, job_key, task_key)
        if self.IsErr(err) {
            return err
        }

        kv := map[string]string{
            "date"       :  "",
            "parcelId"   :  "",
            "bucket"     :  "",
            "imageLevel" :  "",
            "imageType"  :  "",
        }

        taskinfo = Payload{ImageData:make([]Imager, 0)}
        for k := range kv {
            v := b.Get([]byte(k))
            kv[k] = string(v)
        }
        taskinfo.Date, err = strconv.ParseInt(kv["date"], 10, 64)
        if self.IsErr(err) {
            return err
        }
        pid, err := strconv.ParseInt(kv["parcelId"], 10, 64)
        if self.IsErr(err) {
            return err
        }
        taskinfo.ParcelId = int(pid)
        taskinfo.Bucket = kv["bucket"]
        taskinfo.ImageLevel = (&Level{kv["imageLevel"]}).ShortString()
        taskinfo.ImageType = kv["imageType"]

        return nil
    })

    if err != nil {
        PrintlnError(err)
    }
    return taskinfo, err == nil
}

func (self *Store) GetGCSImgs(job_key, task_key string, imgs []string) ([]Imager, bool) {
    var images = make([]Imager, 0)
    err := self.db.View(func(tx *bolt.Tx) error {
        b, err := self.GetImgBucket(tx, job_key, task_key)
        if self.IsErr(err) {
            return err
        }

        for _, k := range imgs {
            v := b.Get([]byte(k))
            var dat = ServerImg{}
            if err := json.Unmarshal(v, &dat); err != nil {
                return err
            }
            images = append(images, dat)
        }
        return nil
    })

    if err != nil {
        PrintlnError(err)
    }
    return images, err == nil
}

func (self *Store) GetImgBucket(tx *bolt.Tx, job_key, task_key string) (*bolt.Bucket, error) {
    jb := tx.Bucket([]byte(job_key))
    if jb == nil {
        return nil, errors.New("undefined job bucket...")
    }
    tb := jb.Bucket([]byte(task_key))
    if tb == nil {
        return nil, errors.New("undefined task bucket")
    }
    b := tb.Bucket([]byte(ImageDataKey))

    return b, nil
}

func (self *Store) GetTaskBucket(tx *bolt.Tx, job_key, task_key string) (*bolt.Bucket, error) {
    jb := tx.Bucket([]byte(job_key))
    if jb == nil {
        return nil, errors.New("undefined job bucket...")
    }
    b := jb.Bucket([]byte(task_key))
    if b == nil {
        return nil, errors.New("undefined task bucket")
    }

    return b, nil
}

func (self *Store) FinalizeDBState(job string) bool {
    alldone := true
    err := self.db.Update(func(tx *bolt.Tx) error {
        jb := tx.Bucket([]byte(job))
        if jb == nil {
            return errors.New("invalid bucket")
        }

        var task_buckets = make([]*bolt.Bucket, 0)

        c := jb.Cursor()

        for k, v := c.Seek([]byte(TaskPrefix)); bytes.HasPrefix(
            k, []byte(TaskPrefix)); k, v = c.Next() {

            if v == nil {
                task_buckets = append(task_buckets, jb.Bucket(k))
            }
        }

        for _, tb := range task_buckets {
            imb := tb.Bucket([]byte(ImageDataKey))
            done := true
            if self.AnyPendingToGCS(imb) {
                done, alldone = false, false
                err := self.MarkNotDone(tb)
                if self.IsErr(err) {
                    return err
                }
            }
            if self.AnyPendingToServer(imb) {
                done, alldone = false, false
                err := self.MarkNotDone(tb)
                if self.IsErr(err) {
                    return err
                }
            }

            if done {
                err := self.MarkDone(tb)
                if self.IsErr(err) {
                    return err
                }
            }
        }
        //after marking all tasks as done or not done
        //if all is still done, mark job as done
        if alldone {
            err := self.MarkDone(jb)
            if self.IsErr(err) {
                return err
            }
        }
        return nil
    })
    if self.IsErr(err) {
        PrintlnError(err)
    }
    return alldone
}

func (self *Store) AnyPendingToGCS(imgB *bolt.Bucket) bool {
    c := imgB.Cursor()
    pending := false
    for k, _ := c.Seek([]byte(ImgPrefix)); bytes.HasPrefix(
        k, []byte(ImgPrefix)); k, _ = c.Next() {
        //if has img prefix -> some pending to be uploaded
        pending = true
        break
    }
    return pending
}

func (self *Store) AnyPendingToServer(imgB *bolt.Bucket) bool {
    c := imgB.Cursor()
    pending := false
    for k, _ := c.Seek([]byte(GCSPrefix)); bytes.HasPrefix(
        k, []byte(GCSPrefix)); k, _ = c.Next() {
        //if has gcs prefix -> some pending to be server
        pending = true
        break
    }
    return pending
}

func MakeImgKey(prefix, id string) string {
    return fmt.Sprintf("%v%v", prefix, id)
}

func ImgIDFromKey(key string) string {
    tokens := strings.Split(key, "_")
    return tokens[len(tokens) - 1]
}

func create_bucket(b *bolt.Bucket, key string) (*bolt.Bucket, error) {
    return b.CreateBucketIfNotExists([]byte(key))
}

func next_id(b *bolt.Bucket) (string, error) {
    id, err := b.NextSequence()
    if err != nil {
        return "", err
    }
    return fmt.Sprintf("%v", id), nil
}






