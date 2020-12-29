import logging
from r2base.processors.bases import ProcessorBase
import numpy as np
from sklearn.decomposition import PCA


class PCAReducer(ProcessorBase):
    logger = logging.getLogger(__name__)

    def run(self, embeddings: np.ndarray, n_components: int = 3) -> np.ndarray:
        if embeddings.shape[1] <= n_components:
            return embeddings

        pca = PCA(n_components=min(n_components, embeddings.shape[0]))
        pca_embedding = pca.fit_transform(embeddings)
        return pca_embedding