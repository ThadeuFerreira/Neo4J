import random
import xml.etree.ElementTree as ET
from neo4j import GraphDatabase
import math

NEO4J_URI = "bolt://neo4j:7687"
#NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "your_password"

def ingest_uniprot_data():
    print("START")
    print("READING FILE!!!")
    tree = ET.parse('data/Q9Y261.xml')
    root = tree.getroot()

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    # Define your functions to parse different elements and create nodes and relationships in Neo4j
    # Example: parse_protein, parse_gene, parse_organism, parse_reference, etc.
    # ...

    with driver.session() as session:
        for entry in root.findall('{http://uniprot.org/uniprot}entry'):
            data = {
                "accessions": parse_accessions(entry),
                "protein": parse_protein(entry),
                "gene": parse_gene(entry),
                "organism": parse_organism(entry),
                "references": parse_reference(entry),
                "features": parse_features(entry),
            }
            protein_id = data["accessions"][0]
            gene_name = data["gene"]["gene_name"]
            organism_id = data["organism"]["dbReference"]["id"]
            session.execute_write(create_protein_node, data["protein"], protein_id)
            session.execute_write(create_gene_node, data["gene"])
            session.execute_write(create_organism_node, data["organism"])
            session.execute_write(create_references, data["references"])
            session.execute_write(create_features, data["features"], protein_id)

            # Create relationships
            session.execute_write(create_protein_gene_relationship, protein_id, gene_name)
            session.execute_write(create_protein_organism_relationship, protein_id, organism_id)

            for reference in data["references"]:
                reference_id = reference["key"]
                session.execute_write(create_protein_reference_relationship, protein_id, reference_id)
            
            for feature in data["features"]:
                feature_id = feature["id"]
                session.execute_write(create_protein_feature_relationship, protein_id, feature_id)

def create_protein_feature_relationship(tx, protein_id, feature_id):
    query = """
    MATCH (p:Protein {id: $protein_id})
    MATCH (f:Feature {id: $feature_id})
    MERGE (p)-[:HAS_FEATURE]->(f)
    """
    tx.run(query, protein_id=protein_id, feature_id=feature_id)

def create_protein_gene_relationship(tx, protein_id, gene_name):
    query = """
    MATCH (p:Protein {id: $protein_id})
    MATCH (g:Gene {name: $gene_id})
    MERGE (p)-[:FROM_GENE]->(g)
    """
    tx.run(query, protein_id=protein_id, gene_id=gene_name)

def create_protein_organism_relationship(tx, protein_id, organism_id):
    query = """
    MATCH (p:Protein {id: $protein_id})
    MATCH (o:Organism {id: $organism_id})
    MERGE (p)-[:FROM_ORGANISM]->(o)
    """
    tx.run(query, protein_id=protein_id, organism_id=organism_id)

def create_protein_reference_relationship(tx, protein_id, reference_id):
    query = """
    MATCH (p:Protein {id: $protein_id})
    MATCH (r:Reference {id: $reference_id})
    MERGE (p)-[:HAS_REFERENCE]->(r)
    """
    tx.run(query, protein_id=protein_id, reference_id=reference_id)

def create_features(tx, features, id):
    for f in features:
        query = """
        MERGE (f:Feature {id: $key, type: $type, description: $description, begin: $begin, end: $end, evidence: $evidence})
        """
        tx.run(query, 
               key=f['id'] if f['id'] else random.randint(0, 100000000),
               type=f['type'] if f['type'] else '', 
               description=f['description'] if f['description'] else '', 
               id=id, begin=f['begin'], 
               end=f['end'],
               evidence=f['evidence'] if f['evidence'] else '')

def create_gene_node(tx, gene_data):
    query = '''
    CREATE (g:Gene {name: $name, synonyms: $synonyms})
    RETURN g
    '''
    tx.run(query, name=gene_data['gene_name'], synonyms=gene_data['gene_synonyms'])

def create_protein_node(tx, protein_data, id):
    query = '''
    CREATE (p:Protein {id: $id, fullName: $fullName})
    RETURN p
    '''
    tx.run(query, id=id, fullName=protein_data['recomended_name']['full_name'])
    
    
def create_organism_node(tx, o):
        query = """
        MERGE (o:Organism {name: $organism_name, common_name: $common_name, dbReference_type: $dbReference_type, id: $taxon_id})
        SET o.lineage = $lineage
        """
        tx.run(query, organism_name=o["scientific_name"], 
                common_name=o["common_name"], 
                dbReference_type=o["dbReference"]["type"], 
                taxon_id=o["dbReference"]["id"], 
                lineage=o["lineage"])

def create_references(tx, references):
    for r in references:
        query = """
        MERGE (r:Reference {id: $key,  
        citation_type: $citation_type, 
        title: $title, 
        date: $date, 
        authors: $authors, 
        journal: $journal, 
        volume: $volume, 
        first_page: $first_page, 
        last_page: $last_page,
        dbReference_type: $dbReference_type,
        dbReference_id: $dbReference_id,
        tissue: $tissue
        })
        """

        dbReference_type_list = [ dbReference['type'] for dbReference in r['dbReference'] ]
        dbReference_id_list = [ dbReference['id'] for dbReference in r['dbReference'] ]
        tx.run(query, key=r['key'],
                citation_type=r['citation_type'], 
                title=r['title'], 
                date=r['date'], 
                authors=r['author_list'],
                journal=r['journal_name'] if r['journal_name'] else '',
                volume=r['volume'] if r['volume'] else '',
                first_page=r['first_page'] if r['first_page'] else '',
                last_page=r['last_page'] if r['last_page'] else '',
                dbReference_type=dbReference_type_list,
                dbReference_id=dbReference_id_list,
                tissue=r['tissue']
                )          



def parse_accessions(entry):
    accessions = entry.findall('{http://uniprot.org/uniprot}accession')
    return [accession.text for accession in accessions]

def parse_gene(entry):
    gene = entry.find('{http://uniprot.org/uniprot}gene')
    gene_name_primary = gene.find('{http://uniprot.org/uniprot}name[@type="primary"]').text
    gene_synonyms = [synonym.text for synonym in gene.findall('{http://uniprot.org/uniprot}name[@type="synonym"]')]
    return {'gene_name': gene_name_primary, 'gene_synonyms': gene_synonyms}

def parse_protein(entry):
    protein = entry.find('{http://uniprot.org/uniprot}protein')
    recommended_name = protein.find('{http://uniprot.org/uniprot}recommendedName')
    recommended_name_full_name = recommended_name.find('{http://uniprot.org/uniprot}fullName').text
    recommended_name_short_names = [short_name.text for short_name in recommended_name.findall('{http://uniprot.org/uniprot}shortName')]
    recommended_name = (recommended_name_full_name, recommended_name_short_names)
    alternative_names = protein.findall('{http://uniprot.org/uniprot}alternativeName')
    alternative_names = [(alternative_name.find('{http://uniprot.org/uniprot}fullName').text, [short_name.text for short_name in alternative_name.findall('{http://uniprot.org/uniprot}shortName')]) for alternative_name in alternative_names]
    #make a dict
    alternative_names_ret = []
    for alternative_name in alternative_names:
        alternative_names_dict = {'full_name': alternative_name[0], 
                                    'short_names': alternative_name[1]
                                }     
        alternative_names_ret.append(alternative_names_dict)
    proteinDict = {
        'recomended_name' :{
            'full_name': recommended_name_full_name,
            'short_names': recommended_name_short_names
        },
        'alternative_names': alternative_names_ret
    }   
    return proteinDict

def parse_features(entry):
    features = entry.findall('{http://uniprot.org/uniprot}feature')
    retFeatures = []
    for feature in features:
        id = feature.get("id")
        type = feature.get("type")
        description = feature.get("description")
        evidence = feature.get("evidence")
        location = feature.find('{http://uniprot.org/uniprot}location')
        position = location.find('{http://uniprot.org/uniprot}position')
        if position is None:
            begin = location.find('{http://uniprot.org/uniprot}begin').get("position")
            end = location.find('{http://uniprot.org/uniprot}end').get("position")
        else:
            begin = position.get("position")
            end = position.get("position")

        featureDict = {
            'id': id,
            'type': type,
            'description': description,
            'begin': begin,
            'end': end,
            'evidence': evidence
        }
        retFeatures.append(featureDict)
    return retFeatures

def parse_reference(entry):
    references = entry.findall('{http://uniprot.org/uniprot}reference')
    retReferences = []
    for reference in references:
        key = reference.get("key")
    
        citation = reference.find("{http://uniprot.org/uniprot}citation")
        citation_type = citation.get("type")
        date = citation.get("date")
        journal_name = citation.get("name")
        volume = citation.get("volume")
        first_page = citation.get("first")
        last_page = citation.get("last")
        title = citation.find("{http://uniprot.org/uniprot}title").text
        
        author_list = [person.get("name") for person in citation.findall("{http://uniprot.org/uniprot}authorList/{http://uniprot.org/uniprot}person")]
        
        db_references = []
        for db_ref in citation.findall("{http://uniprot.org/uniprot}dbReference"):
            reference_type = db_ref.get("type")
            reference_id = db_ref.get("id")
            db_references.append({"type": reference_type, "id": reference_id})
        
        scopes = [scope.text for scope in reference.findall("{http://uniprot.org/uniprot}scope")]
        
        source = reference.findall("{http://uniprot.org/uniprot}source")
        
        sources = []
        for source in source:
            for tissue in source.findall("{http://uniprot.org/uniprot}tissue"):
                sources.append(tissue.text)

        citation = {
                    "key": key,
                    "citation_type": citation_type,
                    "date": date,
                    "journal_name": journal_name,
                    "volume": volume,
                    "first_page": first_page,
                    "last_page": last_page,
                    "title": title,
                    "author_list": author_list,
                    "dbReference": db_references,
                    "scopes": scopes,
                    "tissue": sources
                    }
        retReferences.append(citation)
    return retReferences


def parse_organism(entry):
    organism = entry.find('{http://uniprot.org/uniprot}organism')


    organism_scientific = organism.find('{http://uniprot.org/uniprot}name[@type="scientific"]').text
    organism_common = organism.find('{http://uniprot.org/uniprot}name[@type="common"]').text
    organism_lineage = [taxon.text for taxon in organism.findall('{http://uniprot.org/uniprot}lineage/{http://uniprot.org/uniprot}taxon')]
    organism_dbReference = organism.find('{http://uniprot.org/uniprot}dbReference')
    organism_dbReference_type = organism_dbReference.get("type")
    organism_dbReference_id = organism_dbReference.get("id")

    
    organism_dict = {
        'scientific_name': organism_scientific,
        'common_name': organism_common,
        'lineage': organism_lineage,
        'dbReference': {
            'type': organism_dbReference_type,
            'id': organism_dbReference_id
        }
    }
    return organism_dict



def main():
    ingest_uniprot_data()

if __name__ == "__main__":
    main()