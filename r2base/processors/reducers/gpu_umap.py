import logging
from r2base.processors.bases import ProcessorBase
import numpy as np
from cuml.neighbors import NearestNeighbors
from cuml.manifold import UMAP
import pickle
import redis
import uuid
from r2base.config import EnvVar

class GpuUMAPReducer(ProcessorBase):
    logger = logging.getLogger(__name__)

    def store_model(self, model):
        try:
            client = redis.Redis.from_url(EnvVar.Redis_URL)
            model_idx = str(uuid.uuid4())
            client.set(model_idx, pickle.dumps(model), ex=3600)
        except:
            model_idx = None
        return model_idx

    def run(self, embeddings: np.ndarray,
            n_neighbors: int = 10,
            umap_min_dist: float = 0.01,
            n_components: int = 3,
            umap_metric: str = 'cosine',
            random_seed: int = 42,
            l2_norm: bool = False,
            save_model: bool = False) -> np.ndarray:
        """ Reduce dimensionality of embeddings using UMAP and train a UMAP model

        Parameters
        ----------
        embeddings : np.ndarray
            The extracted embeddings using the sentence transformer module.

        Returns
        -------
        umap_embeddings : np.ndarray
            The reduced embeddings
        """
        if embeddings.shape[1] <= n_components:
            return embeddings

        m = NearestNeighbors(n_neighbors=n_neighbors, metric=umap_metric)
        m.fit(embeddings)
        knn_graph = m.kneighbors(embeddings)

        umap_model = UMAP(n_neighbors=n_neighbors,
                          n_components=n_components,
                          min_dist=umap_min_dist,
                          # metric=umap_metric,
                          random_state=random_seed
                          ).fit(embeddings, knn_graph=knn_graph)
        umap_embeddings = umap_model.transform(embeddings)
        if save_model:
            model_idx = self.store_model(umap_model)
        else:
            model_idx = None
        self.logger.info("Reduced dimensionality with UMAP")
        return umap_embeddings, model_idx


if __name__ == "__main__":
    import  numpy as np
    data = np.random.random(500*200).reshape(500, 200)
    m = GpuUMAPReducer().run(data)
    print(m.shape)