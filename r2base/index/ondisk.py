# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import List
import faiss
import logging

LOG = logging.getLogger(__name__)

def merge_ondisk(trained_index: faiss.Index,
                 shard_fnames: List[str],
                 ivfdata_fname: str) -> None:
    """ Add the contents of the indexes stored in shard_fnames into the index
    trained_index. The on-disk data is stored in ivfdata_fname """
    # merge the images into an on-disk index
    # first load the inverted lists
    ivfs = []
    for fname in shard_fnames:
        # the IO_FLAG_MMAP is to avoid actually loading the data thus
        # the total size of the inverted lists can exceed the
        # available RAM
        LOG.info("read " + fname)
        index = faiss.read_index(fname, faiss.IO_FLAG_MMAP)
        index_ivf = faiss.extract_index_ivf(index)
        ivfs.append(index_ivf.invlists)

        # avoid that the invlists get deallocated with the index
        index_ivf.own_invlists = False

    # construct the output index
    index = trained_index
    index_ivf = faiss.extract_index_ivf(index)

    assert index.ntotal == 0, "works only on empty index"

    # prepare the output inverted lists. They will be written
    # to merged_index.ivfdata
    invlists = faiss.OnDiskInvertedLists(
        index_ivf.nlist, index_ivf.code_size,
        ivfdata_fname)

    # merge all the inverted lists
    ivf_vector = faiss.InvertedListsPtrVector()
    for ivf in ivfs:
        ivf_vector.push_back(ivf)

    LOG.info("merge %d inverted lists " % ivf_vector.size())
    ntotal = invlists.merge_from(ivf_vector.data(), ivf_vector.size())

    # now replace the inverted lists in the output index
    index.ntotal = index_ivf.ntotal = ntotal
    index_ivf.replace_invlists(invlists, True)
    invlists.this.disown()



if __name__ == '__main__':
    stage = 6
    from r2base.index.ondisk import merge_ondisk
    tmpdir = './'

    if stage == 0:
        # train the index
        xt = np.random.random(10000).reshape(500, 20).astype('float32')
        index = faiss.index_factory(xt.shape[1], "IVF500,Flat")
        print("training index")
        index.train(xt)
        print("write " + tmpdir + "trained.index")
        print(index.ntotal)
        index.add_with_ids(xt, np.arange(0, xt.shape[0]))
        print(index.ntotal)
        faiss.write_index(index, tmpdir + "trained.index")

    if 1 <= stage <= 4:
        # add 1/4 of the database to 4 independent indexes
        bno = stage - 1
        xb = fvecs_read("sift1M/sift_base.fvecs")
        i0, i1 = int(bno * xb.shape[0] / 4), int((bno + 1) * xb.shape[0] / 4)
        index = faiss.read_index(tmpdir + "trained.index")
        print("adding vectors %d:%d" % (i0, i1))
        index.add_with_ids(xb[i0:i1], np.arange(i0, i1))
        print("write " + tmpdir + "block_%d.index" % bno)
        faiss.write_index(index, tmpdir + "block_%d.index" % bno)

    if stage == 5:
        print('loading trained index')
        # construct the output index
        index = faiss.read_index(tmpdir + "trained.index")

        block_fnames = [
            tmpdir + "block_%d.index" % bno
            for bno in range(4)
        ]

        merge_ondisk(index, block_fnames, tmpdir + "merged_index.ivfdata")

        print("write " + tmpdir + "populated.index")
        faiss.write_index(index, tmpdir + "populated.index")

    if stage == 6:
        # perform a search from disk
        print("read " + tmpdir + "trained.index")
        index = faiss.read_index(tmpdir + "trained.index")
        index.nprobe = 16

        xq = np.random.random(100).reshape(5, 20).astype('float32')
        # load query vectors and ground-truth
        D, I = index.search(xq, 5)
        print(D)
        print(I)

    exit()