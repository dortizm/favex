document.addEventListener('DOMContentLoaded', () => {

    /* ==========================================================================
       1. LÓGICA DEL CARRUSEL EXTERIOR (Movimiento de Miniaturas)
       ========================================================================== */
    const slider = document.getElementById('faveGallerySlider');
    const btnLeft = document.getElementById('slideLeftBtn');
    const btnRight = document.getElementById('slideRightBtn');

 
    if (slider && btnLeft && btnRight) {
        const scrollAmount = 240; 
        
        btnLeft.addEventListener('click', () => {
            slider.scrollBy({ left: -scrollAmount, behavior: 'smooth' });
        });

        btnRight.addEventListener('click', () => {
            slider.scrollBy({ left: scrollAmount, behavior: 'smooth' });
        });
    }

    /* ==========================================================================
       2. LÓGICA DEL MODAL FULL SCREEN (Visor, Spinner y Navegación Blindada)
       ========================================================================== */
    const imageLightboxModal = document.getElementById('imageLightboxModal');
    const expandedGalleryImg = document.getElementById('expandedGalleryImg');
    const imageLoadingSpinner = document.getElementById('imageLoadingSpinner');
    
    const modalPrevBtn = document.getElementById('modalPrevBtn');
    const modalNextBtn = document.getElementById('modalNextBtn');

   
    if (imageLightboxModal && expandedGalleryImg) {
        
        const thumbnails = Array.from(document.querySelectorAll('.gallery-thumb'));
        let currentImageIndex = 0; 

      
        function loadModalImage(index) {
            if (thumbnails.length === 0) return; 
            
           
            if (index < 0) index = thumbnails.length - 1;
            if (index >= thumbnails.length) index = 0;
            
            currentImageIndex = index; 
            const highResImageSrc = thumbnails[currentImageIndex].getAttribute('data-large-src');

         
            imageLoadingSpinner.classList.remove('d-none');
            expandedGalleryImg.style.transition = 'opacity 0.3s ease';
            expandedGalleryImg.style.opacity = '0.4'; 

         
            const tempImage = new Image();
            
            tempImage.onload = function() {
                expandedGalleryImg.setAttribute('src', highResImageSrc);
                expandedGalleryImg.style.opacity = '1';
                imageLoadingSpinner.classList.add('d-none');
            };

            tempImage.onerror = function() {
                console.error('No se encontró la imagen: ' + highResImageSrc);
                expandedGalleryImg.style.opacity = '1';
                imageLoadingSpinner.classList.add('d-none');
            };

            tempImage.src = highResImageSrc;
        }

        // Al abrir el modal pinchando una miniatura
        imageLightboxModal.addEventListener('show.bs.modal', function (event) {
            const clickedThumbnail = event.relatedTarget;
            const index = thumbnails.indexOf(clickedThumbnail);
            if(index !== -1) loadModalImage(index);
        });

        // Botón Anterior dentro del Modal
        if (modalPrevBtn) {
            modalPrevBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation(); 
                loadModalImage(currentImageIndex - 1);
            });
        }
        
        // Botón Siguiente dentro del Modal
        if (modalNextBtn) {
            modalNextBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                loadModalImage(currentImageIndex + 1);
            });
        }

        // Navegación con Teclado
        document.addEventListener('keydown', (e) => {
            if (imageLightboxModal.classList.contains('show')) {
                if (e.key === 'ArrowLeft') loadModalImage(currentImageIndex - 1);
                if (e.key === 'ArrowRight') loadModalImage(currentImageIndex + 1);
            }
        });

        // Limpieza al cerrar
        imageLightboxModal.addEventListener('hidden.bs.modal', function () {
            expandedGalleryImg.setAttribute('src', '');
            expandedGalleryImg.style.opacity = '0';
            imageLoadingSpinner.classList.remove('d-none');
        });
    }

});