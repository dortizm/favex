/* ==========================================================================
   1. CONFIGURACIÓN Y DATOS
   ========================================================================== */
const docsData = {
    articulos: [
        { 
            title: "Libro Rojo de la Flora Nativa: Región de Coquimbo", 
            desc: "Descripción del estado de conservación.", 
            archivo: "LIBRO ROJO DE LA FLORA NATIVA_COQUIMBO.pdf" 
        },
        { 
            title: "Libro Rojo de la Flora Nativa: Región de Atacama", 
            desc: "Descripción detallada de la región.",
            archivo: "LIBRO ROJO DE LA FLORA NATIVA_ATACAMA.pdf" 
        },   
        { 
            title: "Efecto del riego y la poda en la captura de niebla", 
            desc: "Estudio sobre formaciones xerofíticas.",
            archivo: "Vista de Efecto del riego y la poda en la habilidad de captura de niebla de formaciones xerofíticas chilenas.pdf" 
        }
    ],
    eia: [
        { 
            title: "Resumen ejecutivo", 
            desc: "Estudio de impacto ambiental “Proyecto Volta”.",
            archivo: "Resumen_Ejecutivo_EIA_Volta.pdf" 
        }
    ],
    informes: [
        { 
            title: "Sexto Informe nacional de biodiversidad", 
            desc: "Reporte oficial.",
            archivo: "8-sexto-informe-nacional-de-biodiversidad.pdf" 
        },
        { 
            title: "Catastro de formaciones xerofíticas", 
            desc: "Áreas prioritarias.",
            archivo: "CATASTRO DE FORMACIONES XEROFÍTICAS EN ÁREAS PRIORITARIAS_200.pdf" 
        }
    ],
    normativos: [
        { 
            title: "Ley 20283 - Bosque Nativo", 
            desc: "Ley sobre recuperación y fomento forestal.", 
            archivo: "Ley 20283_LEY SOBRE RECUPERACIÓN DEL BOSQUE NATIVO Y FOMENTO FORESTAL.pdf" 
        }
    ]
};

/* ==========================================================================
   2. REFERENCIAS Y LÓGICA
   ========================================================================== */
const filterSelect = document.getElementById('docFilter');
const sectionTitle = document.getElementById('sectionTitle');
const cardsContainer = document.getElementById('cardsContainer');

// MAPA DE CARPETAS:
// Relaciona la categoría (código) con el Nombre Real de la Carpeta (Explorador)
const folderMap = {
    articulos: "Articulos",    // value="articulos" -> Carpeta "Articulos"
    eia: "EIA",                // value="eia"       -> Carpeta "EIA"
    informes: "Informes",      // value="informes"  -> Carpeta "Informes"
    normativos: "Normativos"   // value="normativos"-> Carpeta "Normativos"
};

function renderDocuments(category) {
    const selectedOptionText = filterSelect.options[filterSelect.selectedIndex].text;
    sectionTitle.textContent = selectedOptionText;
    cardsContainer.innerHTML = '';
    
    // 1. Obtenemos los datos
    const items = docsData[category];
    
    // 2. Obtenemos el nombre correcto de la subcarpeta
    const subCarpeta = folderMap[category];

    if (items && items.length > 0) {
        items.forEach(item => {
            
            // 3. Construimos la ruta CORRECTA incluyendo la subcarpeta
            // Ruta: ./assets/PDF/NombreSubcarpeta/NombreArchivo.pdf
            const rutaFinal = `./assets/PDF/${subCarpeta}/${item.archivo}`;

            const cardHTML = `
                <div class="col-12 col-md-6">
                    <div class="card h-100 border border-dark border-opacity-25 rounded-2 shadow-sm doc-card bg-white">
                        <div class="card-body p-4 text-center d-flex flex-column align-items-center">
                            
                            <h5 class="fw-bold mb-3 fs-6">${item.title}</h5>
                            <p class="text-dark small mb-4 text-start w-100">${item.desc}</p>
                            
                            <a href="${rutaFinal}" 
                               class="btn btn-sage w-100 mt-auto btn-sm text-white" 
                               download="${item.archivo}" 
                               target="_blank">
                               Descargar
                            </a>

                        </div>
                    </div>
                </div>
            `;
            cardsContainer.innerHTML += cardHTML;
        });
    } else {
        cardsContainer.innerHTML = '<p class="text-center w-100 py-5">No hay documentos en esta categoría.</p>';
    }
}

filterSelect.addEventListener('change', (e) => {
    renderDocuments(e.target.value);
});

renderDocuments('articulos');