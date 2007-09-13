# Elixir event statements, improved again by beachcoder
# http://pylonshq.com/pasties/258
# 
# For more information on Elixir statements, see
# http://cleverdevil.org/computing/52/

from elixir.statements     import Statement
from sqlalchemy.orm.mapper import MapperExtension
import types

def proxy_to_instance(name):
    def new_func(self, mapper, connection, instance):
        if hasattr(instance, name) : getattr(instance, name)()  
    return new_func

def mapper_extension_statement(name):
    def init(self, entity, *callbacks):
        def proxy_method(self):
            for callback in callbacks:
                getattr(self, callback)()
        setattr(entity, '__%s__' %name, proxy_method)
        extensions = entity._descriptor.mapper_options.get('extension', [])
        if type(extensions) is not types.ListType: 
            extensions = [extensions]
        if ext_proxy not in extensions:
            extensions.append(ext_proxy)
            entity._descriptor.mapper_options['extension'] = extensions
    return type('MapperExtensionStatement', (object, ), {'__init__':init})

class MapperExtensionProxy(MapperExtension):
        
    after_delete = proxy_to_instance('__after_delete__')
    after_insert = proxy_to_instance('__after_insert__')
    after_update = proxy_to_instance('__after_update__')
    before_delete = proxy_to_instance('__before_delete__')
    before_insert = proxy_to_instance('__before_insert__')
    before_update = proxy_to_instance('__before_update__')
            
ext_proxy = MapperExtensionProxy()

after_delete = Statement(mapper_extension_statement('after_delete'))            
after_insert = Statement(mapper_extension_statement('after_insert'))            
after_update = Statement(mapper_extension_statement('after_update'))            
before_delete = Statement(mapper_extension_statement('before_delete'))          
before_insert = Statement(mapper_extension_statement('before_insert'))          
before_update = Statement(mapper_extension_statement('before_update'))

if __name__ == '__main__':

    from elixir import *
    
    class Car(Entity):
        has_field('name', String(30))
        before_insert('before_insert_test')
        before_update('before_update_test')
        before_delete('before_delete_test')
        after_insert('after_insert_test')
        after_update('after_update_test')
        after_delete('after_delete_test')
        def before_insert_test(self): print "%s before insert" %self.name
        def before_update_test(self): print "%s before update" %self.name
        def before_delete_test(self): print "%s before delete" %self.name
        def after_insert_test(self): print "%s after insert" %self.name
        def after_update_test(self): print "%s after update" %self.name
        def after_delete_test(self): print "%s after delete" %self.name

    metadata.connect('sqlite:///test_events.db')
    metadata.drop_all()
    metadata.create_all()
    
    car = Car(name = 'Ferrari')
    car.flush()
    car.name = 'Lotus'
    car.flush()
    car.delete()
    car.flush()
