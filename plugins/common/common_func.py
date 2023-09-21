def get_sftp():
    print("sftp 작업을 시작합니다.")

# *args 오퍼레이터에서 *args 활용법
def regist(name, sex, *args):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타: {args}')


def regist2(name, sex, *args, **kwargs):
    print(f'이름 : {name}')
    print(f'성별 : {sex}')
    print(f'기타옵션 : {args}')

    # kwargs 의 값 호출
    email = kwargs.get('email', None) # 해당 키 값이 존재한다면 그 값을 email 변수에 할당
    phone = kwargs.get('phone', None) # 없으면 None 

    # 조건에 따른 출력 
    if email:
        print(email)
    
    if phone:
        print(phone)


