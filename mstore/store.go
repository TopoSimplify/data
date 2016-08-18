package storage

import (
    "github.com/boltdb/bolt"
    "log"
    "encoding/json"
    "fmt"
    "bytes"
    "errors"
    "strconv"
    "strings"
)

const ImageDataKey = "imageData"


type Store struct {
    db *bolt.DB
}

func NewStorage(db *bolt.DB) *Store {
    if db == nil {
        log.Fatalln("storage db is not defined")
    }
    return &Store{db:db}
}



func (store *Store) MarkDone(b *bolt.Bucket) error {
    return b.Put([]byte("done"), []byte(Done))
}

func (store *Store) MarkNotDone(b *bolt.Bucket) error {
    return b.Put([]byte("done"), []byte(NotDone))
}

func (store *Store) IsDone(b *bolt.Bucket) bool {
    state := b.Get([]byte("done"))
    return string(state) == Done
}



//saves tasks info (includes creating imageData bucket on task)
func (store *Store) SaveTaskInfo(b *bolt.Bucket, task *Payload) error {
    var err error = nil
    // create imageData bucket on bucket
    _, err = create_sub_bucket(b, ImageDataKey)
    if store.IsErr(err) {
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
        if err = store.Put(b, k, val); store.IsErr(err) {
            return err
        }
    }

    return store.MarkNotDone(b)
}

//saves images in a given task payload
func (store *Store) SaveImageInfo(b *bolt.Bucket, task *Payload) error {
    var err error = nil
    b, err = b.CreateBucketIfNotExists([]byte(ImageDataKey))
    if store.IsErr(err) {
        return err
    }

    imgdata := task.ImageData
    for _, img := range imgdata {
        k, err := next_id(b)
        if store.IsErr(err) {
            return err
        }
        //img key
        k = MakeImgKey(ImgPrefix, k)

        v, err := json.Marshal(img)
        if store.IsErr(err) {
            return err
        }

        if err = store.Put(b, k, string(v)); store.IsErr(err) {
            return err
        }
    }

    return nil
}

//returns all job buckets
func (store *Store) AllJobs() []string {
    keys := make([]string, 0)
    err := store.db.View(func(tx *bolt.Tx) error {
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
func (store *Store) LatestJob() string {
    var key string
    err := store.db.View(func(tx *bolt.Tx) error {
        c := tx.Cursor()
        prefix := []byte(JobPrefix)
        for k, v := c.Last(); k != nil; k, v = c.Prev() {
            if v == nil && bytes.HasPrefix(k, prefix) {
                b := tx.Bucket(k)
                if b != nil && !store.IsDone(b) {
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
func (store *Store) DeleteBucket(bucket string) bool {
    key := []byte(bucket)
    err := store.db.Update(func(tx *bolt.Tx) error {
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
func (store *Store) CancelJob(bucket string) bool {
    key := []byte(bucket)
    err := store.db.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket(key)
        if b != nil {
            return store.MarkDone(b)
        }
        return nil
    })
    if err != nil {
        log.Fatalln(err)
    }
    return true
}

//returns all job buckets
func (store *Store) GetGCSPayload(job string) []*UpObj {
    var path = job + "|"

    var gcs_payload = make([]*UpObj, 0)
    job_key := []byte(job)

    err := store.db.View(func(tx *bolt.Tx) error {

        jb := tx.Bucket(job_key)
        if jb == nil {
            PrintlnWarning("undefined job bucket...")
            return nil
        }

        tasks := store.GetAllTasksNotDone(jb)

        for _, tk := range tasks {
            tk_path := path + string(tk) + "|"
            tb := jb.Bucket(tk)
            imb := tb.Bucket([]byte(ImageDataKey))

            tsk_imgs, err := store.GetAllImgData(imb)
            if store.IsErr(err) {
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
func (store *Store) GetServerPayload(job string) []*UpObj {
    var path = job + "|"

    var gcs_payload = make([]*UpObj, 0)
    job_key := []byte(job)

    err := store.db.View(func(tx *bolt.Tx) error {

        jb := tx.Bucket(job_key)
        if jb == nil {
            PrintlnWarning("undefined job bucket...")
            return nil
        }

        tasks := store.GetAllTasksNotDone(jb)

        for _, tk := range tasks {
            tk_path := path + string(tk) + "|"
            tb := jb.Bucket(tk)
            imb := tb.Bucket([]byte(ImageDataKey))

            tsk_imgs, err := store.GetAllGCSData(imb)
            if store.IsErr(err) {
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

func (store *Store) GetAllTasksNotDone(b *bolt.Bucket) [][]byte {
    var tasks = make([][]byte, 0)
    c := b.Cursor()
    for k, v := c.Seek([]byte(TaskPrefix)); bytes.HasPrefix(
        k, []byte(TaskPrefix)); k, v = c.Next() {
        if v == nil {
            tb := b.Bucket(k)
            if !store.IsDone(tb) {
                tasks = append(tasks, k)
            }
        }
    }
    return tasks
}

func (store *Store) GetAllImgData(b *bolt.Bucket) (map[string]Img, error) {
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

func (store *Store) GetAllGCSData(b *bolt.Bucket) (map[string]Img, error) {
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

func (store *Store) SetGCSImg(job_key, task_key, img_key string) bool {
    err := store.db.Update(func(tx *bolt.Tx) error {
        b, err := store.GetImgBucket(tx, job_key, task_key)
        if store.IsErr(err) {
            return err
        }
        k := img_key
        v := b.Get([]byte(k))
        if len(v) > 0 {
            err = b.Delete([]byte(k))
            if store.IsErr(err) {
                return err
            }
            k = MakeImgKey(GCSPrefix, ImgIDFromKey(k))
            return store.Put(b, k, string(v))
        }
        return nil
    })

    if err != nil {
        PrintlnError(err)
    }
    return err == nil
}

//update a list of keys
func (store *Store) SetGCSImgs(keys []string) bool {
    err := store.db.Update(func(tx *bolt.Tx) error {
        for _, key := range keys {
            tokens := strings.Split(key, "|")
            job_key, task_key, img_key := tokens[0], tokens[1], tokens[2]

            b, err := store.GetImgBucket(tx, job_key, task_key)
            if store.IsErr(err) {
                return err
            }

            k := img_key
            v := b.Get([]byte(k))
            if len(v) > 0 {
                err = b.Delete([]byte(k))
                if store.IsErr(err) {
                    return err
                }
                k = MakeImgKey(GCSPrefix, ImgIDFromKey(k))
                err := store.Put(b, k, string(v))
                if store.IsErr(err) {
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

func (store *Store) SetServerImgs(job_key, task_key string, img_keys []string) bool {
    err := store.db.Update(func(tx *bolt.Tx) error {
        b, err := store.GetImgBucket(tx, job_key, task_key)
        if store.IsErr(err) {
            return err
        }
        for _, k := range img_keys {
            v := b.Get([]byte(k))
            if len(v) > 0 {

                err = b.Delete([]byte(k))

                if store.IsErr(err) {
                    PrintlnError(err)
                    return err
                }

                k := MakeImgKey(ServerPrefix, ImgIDFromKey(k))
                err := store.Put(b, k, string(v))
                if store.IsErr(err) {
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

func (store *Store) GetServerTaskInfo(job_key, task_key string) (Payload, bool) {
    var taskinfo Payload
    err := store.db.View(func(tx *bolt.Tx) error {
        b, err := store.GetTaskBucket(tx, job_key, task_key)
        if store.IsErr(err) {
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
        if store.IsErr(err) {
            return err
        }
        pid, err := strconv.ParseInt(kv["parcelId"], 10, 64)
        if store.IsErr(err) {
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

func (store *Store) GetGCSImgs(job_key, task_key string, imgs []string) ([]Imager, bool) {
    var images = make([]Imager, 0)
    err := store.db.View(func(tx *bolt.Tx) error {
        b, err := store.GetImgBucket(tx, job_key, task_key)
        if store.IsErr(err) {
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

func (store *Store) GetImgBucket(tx *bolt.Tx, job_key, task_key string) (*bolt.Bucket, error) {
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

func (store *Store) GetTaskBucket(tx *bolt.Tx, job_key, task_key string) (*bolt.Bucket, error) {
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

func (store *Store) FinalizeDBState(job string) bool {
    alldone := true
    err := store.db.Update(func(tx *bolt.Tx) error {
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
            if store.AnyPendingToGCS(imb) {
                done, alldone = false, false
                err := store.MarkNotDone(tb)
                if store.IsErr(err) {
                    return err
                }
            }
            if store.AnyPendingToServer(imb) {
                done, alldone = false, false
                err := store.MarkNotDone(tb)
                if store.IsErr(err) {
                    return err
                }
            }

            if done {
                err := store.MarkDone(tb)
                if store.IsErr(err) {
                    return err
                }
            }
        }
        //after marking all tasks as done or not done
        //if all is still done, mark job as done
        if alldone {
            err := store.MarkDone(jb)
            if store.IsErr(err) {
                return err
            }
        }
        return nil
    })
    if store.IsErr(err) {
        PrintlnError(err)
    }
    return alldone
}

func (store *Store) AnyPendingToGCS(imgB *bolt.Bucket) bool {
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

func (store *Store) AnyPendingToServer(imgB *bolt.Bucket) bool {
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


func next_id(b *bolt.Bucket) (string, error) {
    id, err := b.NextSequence()
    if err != nil {
        return "", err
    }
    return fmt.Sprintf("%v", id), nil
}






