const { Firestore } = require('@google-cloud/firestore')
const { Storage } = require('@google-cloud/storage')

const PROJECT_ID = 'streetswipe-aoe'
const SERVICE_KEY = process.env.SERVICE_KEY || '~/.google-cloud/streetswipe-aoe-5627f5cc075a.json'
const STORAGE_BUCKET = 'aesthetics-of-exclusion'

const storage = new Storage({
  keyFilename: SERVICE_KEY,
  projectId: PROJECT_ID
})

const bucket = storage.bucket(STORAGE_BUCKET)

const db = new Firestore({
  keyFilename: SERVICE_KEY,
  projectId: PROJECT_ID
})

const getPoiRef = (poiId) => db.collection('pois').doc(poiId)

async function addAnnotation (poiId, type, data) {
  try {
    const poiRef = getPoiRef(poiId)

    const annotationRef = await poiRef.collection('annotations').doc().set({
      // city: 'amsterdam',
      poiId,
      dateCreated: new Date(),
      dateUpdated: new Date(),
      random: random(),
      type,
      data
    })

    return annotationRef
  } catch (err) {
    throw err
  }
}

async function getAnnotations (poiId, types) {
  let annotationsRef = getPoiRef(poiId).collection('annotations')

  if (types) {
    annotationsRef = annotationsRef.where('type', 'in', types)
  }

  const annotations = await annotationsRef.get()
  return annotations
}

async function deleteAnnotations (poiId, type) {
  const poiRef = getPoiRef(poiId)
  let annotationsRef = poiRef.collection('annotations')
  if (type) {
    annotationsRef = annotationsRef.where('type', '==', type)
  }

  return new Promise((resolve, reject) => {
    deleteAnnotationsBatch(annotationsRef, resolve, reject)
  })
}

function random () {
  return Math.round(Math.random() * Number.MAX_SAFE_INTEGER)
}

function deleteAnnotationsBatch (query, resolve, reject) {
  query.get()
    .then((snapshot) => {
      // When there are no documents left, we are done
      if (snapshot.size === 0) {
        return 0
      }

      // Delete documents in a batch
      let batch = db.batch()
      snapshot.docs.forEach((doc) => {
        batch.delete(doc.ref)
      })

      return batch.commit().then(() => {
        return snapshot.size
      })
    }).then((numDeleted) => {
      if (numDeleted === 0) {
        resolve()
        return
      }

      // Recurse on the next process tick, to avoid
      // exploding the stack.
      process.nextTick(() => {
        deleteAnnotationsBatch(query, resolve, reject)
      })
    })
    .catch(reject)
}

function uploadFile (city, poiId, annotationType, buffer, filename, contentType) {
  return new Promise((resolve, reject) => {
    const bucketName = `${city}/${poiId}/${annotationType}/${filename}`

    const blob = bucket.file(bucketName)
    const blobStream = blob.createWriteStream({
      metadata: {
        contentType
      },
      resumable: false
    })

    blobStream.on('finish', () => {
      const publicUrl = `https://storage.googleapis.com/${bucket.name}/${blob.name}`

      resolve({
        bucket: bucket.name,
        path: blob.name,
        url: publicUrl
      })
    })
      .on('error', (err) => {
        reject(err)
      })
      .end(buffer)
  })
}

module.exports = {
  db,
  random,
  getPoiRef,
  addAnnotation,
  deleteAnnotations,
  getAnnotations,
  uploadFile
}
