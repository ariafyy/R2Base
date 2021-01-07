import logging
from r2base.processors.bases import ProcessorBase
import numpy as np
import umap


class UMAPReducer(ProcessorBase):
    logger = logging.getLogger(__name__)

    def run(self, embeddings: np.ndarray,
            n_neighbors: int = 10,
            umap_min_dist: float = 0.01,
            n_components: int = 3,
            umap_metric: str = 'cosine',
            random_seed: int = 42) -> np.ndarray:
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

        umap_model = umap.UMAP(n_neighbors=n_neighbors,
                               n_components=n_components,
                               min_dist=umap_min_dist,
                               metric=umap_metric,
                               random_state=random_seed
                               ).fit(embeddings)
        umap_embeddings = umap_model.transform(embeddings)
        self.logger.info("Reduced dimensionality with UMAP")
        return umap_embeddings
