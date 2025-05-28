# -*- coding: utf-8 -*-

# -----------------------Import external libraries-----------------------

import pandas as pd  # for importing fom txt file
import numpy as np  # for math function
import numpy.polynomial.polynomial as poly
import os
from os.path import basename
import sys
import file_management as fmgmt
# -----------------------Define functions used in the code-----------------------


def import_raw_data_from_file(file_path):
    # In the vload_array, every row i [i,:,:] corresponds to a single flash pulse;
    # the [:,1,:] is reference cell voltage, which divided by ref const gives intensity;
    # [:,2,:] is voltage; [:,3,:] is current; [:,4,:] is temperature

    #startNew = time.time()

    def find(word, number=0, splitFilter=True):
        # Search the file for row containing 'word'. Separate by " and select
        # second entry in split list. number refers to item in list.
        if splitFilter:
            found = list(filter(lambda a: word in a, content))[
                number].split('"')[1]
        else:
            found = list(filter(lambda a: word in a, content))[
                number].split('"')
        return found

    with open(file_path, 'rt', encoding='mac_latin2') as f:
        content = f.read().splitlines()

    data = dict()

    # Previous method used magic numbers which depended on software version.
    # Test run took average 60ms
    # Updated version with 'find' function can read from any software version.
    # Test run for updated took average 66ms

    # Import Module Inputs
    data['resistivity'] = float(find('Resistivity (ohm-cm)'))
    data['sample_type'] = str(find('Sample Type'))
    data['thickness'] = float(find('Thickness (cm)'))
    data['cell_area'] = float(find('Cell Area'))
    data['module_area'] = float(find('Total Area'))
    data['number_of_cells_per_string'] = float(
        find('Number of Cells per String'))
    data['number_of_strings'] = float(find('Number of Strings'))
    data['number_of_cells'] = float(
        data['number_of_cells_per_string']*data['number_of_strings'])
    data['active_area'] = float(data['cell_area']*data['number_of_cells'])

    # Import Raw Voltage Transfer Parameters
    data['current_transfer'] = float(find('Current Transfer = '))
    data['voltage_transfer'] = float(find('Voltage Transfer = '))
    data['temperature_transfer'] = float(find('Temperature Transfer = '))

    # Import Calibration Inputs

    data['reference_constant'] = float(find('Reference Constant (V/sun)'))
    data['voltage_temperature_coefficienct'] = float(
        find('Voltage Temperature Coefficient'))
    data['temperature_offset'] = float(find('Temperature Offset'))

    # Import Measured Temperature
    data['temperature'] = float(find('Temperature (C)', number=1))

    # Import Measured Vload Data
    content_Vload = find('Averaged Data Out (VLoad Array)', splitFilter=False)
    content_Vload_size_x = int(content_Vload[1].split('>')[
                               0].split('=')[1].split(' ')[0])
    content_Vload_size_y = int(content_Vload[1].split('>')[
                               0].split('=')[1].split(' ')[1])
    content_Vload_size_z = int(content_Vload[1].split('>')[
                               0].split('=')[1].split(' ')[2])
    content_Vload_data = content_Vload[1].split('>')[1].split('"')[
        0].split(' ')

    data_Vload = np.array(content_Vload_data[1:len(content_Vload_data)])
    data_Vload_reshaped = data_Vload.reshape(content_Vload_size_x,
                                             content_Vload_size_y,
                                             content_Vload_size_z)
    data_Vload_reshaped = data_Vload_reshaped.astype(float)

    data['vload_number_of_load_conditions'] = content_Vload_size_x
    data['vload_number_of_points_per_flash'] = content_Vload_size_z
    data['vload_array_raw'] = data_Vload_reshaped
    # vload_array_raw is a [X by Y by Z] array
    # X = number of load conditions
    # Y = 5 (0 = Time(sec), 1 = Uncorrected Intensity, 2 = Uncorrected Voltage, 3 = Uncorrected Current, 4 = Uncorrected Temperature)
    # Z = number of points per flash

    # Import Measured Suns-Voc Data
    content_Voc = find('Averaged Data Out (Voc Array)', splitFilter=False)
    #content_Voc_title = content_Voc[0].split('=')[0]
    content_Voc_size_x = int(content_Voc[1].split('>')[
                             0].split('=')[1].split(' ')[0])
    content_Voc_size_y = int(content_Voc[1].split('>')[
                             0].split('=')[1].split(' ')[1])
    content_Voc_data = content_Voc[1].split('>')[1].split('"')[0].split(' ')
    data_Voc = np.array(content_Voc_data[1:len(content_Voc_data)])
    data_Voc_reshaped = data_Voc.reshape(
        content_Voc_size_x, content_Voc_size_y)
    data_Voc_reshaped = data_Voc_reshaped.astype(float)

    data['voc_number_of_points_per_flash'] = content_Voc_size_y
    data['voc_array_raw'] = data_Voc_reshaped
    # voc_array_raw is a [X by Y] array
    # X = 5 (0 = Time(sec), 1 = Uncorrected Intensity, 2 = Uncorrected Voltage, 3 = Uncorrected Current, 4 = Uncorrected Temperature)
    # Y = number of points per flash

    # Import Measured Suns Isc Data
    content_Isc = find('Averaged Data Out (Isc Array)', splitFilter=False)
    #content_Isc_title = content_Isc[0].split('=')[0]
    content_Isc_size_x = int(content_Isc[1].split('>')[
                             0].split('=')[1].split(' ')[0])
    content_Isc_size_y = int(content_Isc[1].split('>')[
                             0].split('=')[1].split(' ')[1])
    content_Isc_data = content_Isc[1].split('>')[1].split('"')[0].split(' ')
    data_Isc = np.array(content_Isc_data[1: len(content_Isc_data)])
    data_Isc_reshaped = data_Isc.reshape(
        content_Isc_size_x, content_Isc_size_y)
    data_Isc_reshaped = data_Isc_reshaped.astype(float)

    data['isc_number_of_points_per_flash'] = content_Isc_size_y
    data['isc_array_raw'] = data_Isc_reshaped
    # isc_array_raw is a [X by Y] array
    # X = 5 (0 = Time(sec), 1 = Uncorrected Intensity, 2 = Uncorrected Voltage, 3 = Uncorrected Current, 4 = Uncorrected Temperature)
    # Y = number of points per flash

    #endNew = time.time()
    #print(f'New method: {1000*(endNew - startNew)}ms')

    return data, content


def correct_raw_data(raw_data, reference_constant=None,
                     voltage_temperature_coefficient=None,
                     temperature_offset=None):

    # create new variable for corrected data
    data = raw_data.copy()

    # Load calibration inputs if none are specified
    if not reference_constant:
        reference_constant = data['reference_constant']
    if not voltage_temperature_coefficient:
        voltage_temperature_coefficient = data['voltage_temperature_coefficienct']
    if not temperature_offset:
        temperature_offset = data['temperature_offset']
        if temperature_offset > 0:
            temperature_offset = -temperature_offset

    # Update calibration inputs if specified
    data['reference_constant'] = float(reference_constant)
    data['voltage_temperature_coefficienct'] = float(
        voltage_temperature_coefficient)
    data['temperature_offset'] = float(temperature_offset)

    # initialize arrays for corrected data
    data['vload_array'] = data['vload_array_raw'].copy()
    data['voc_array'] = data['voc_array_raw'].copy()
    data['isc_array'] = data['isc_array_raw'].copy()

    # Correct data excluding temperature considerations
    data['vload_array'][:, 1, :] = data['vload_array'][:, 1, :] / \
        data['reference_constant']
    data['vload_array'][:, 2, :] = data['vload_array'][:, 2, :] * \
        data['voltage_transfer']
    data['vload_array'][:, 3, :] = data['vload_array'][:, 3, :] / \
        data['current_transfer']
    data['vload_array'][:, 4, :] = data['vload_array'][:, 4, :] / \
        data['temperature_transfer']

    data['voc_array'][1, :] = data['voc_array'][1, :] / \
        data['reference_constant']
    data['voc_array'][2, :] = data['voc_array'][2, :]*data['voltage_transfer']
    data['voc_array'][3, :] = data['voc_array'][3, :]/data['current_transfer']
    data['voc_array'][4, :] = data['voc_array'][4, :] / \
        data['temperature_transfer']

    data['isc_array'][1, :] = data['isc_array'][1, :] / \
        data['reference_constant']
    data['isc_array'][2, :] = data['isc_array'][2, :]*data['voltage_transfer']
    data['isc_array'][3, :] = data['isc_array'][3, :]/data['current_transfer']
    data['isc_array'][4, :] = data['isc_array'][4, :] / \
        data['temperature_transfer']

    # Correct data with temperature
    temperature_offset = 25 - \
        (data['temperature'] - data['temperature_offset'])
    temperature_correction_factor = data['number_of_cells_per_string'] * \
        data['voltage_temperature_coefficienct']/1000 * temperature_offset
    # temperature_correction_factor in cells*degC*V/cell degC -> V

    data['vload_array'][:, 2, :] = data['vload_array'][:, 2, :] - \
        temperature_correction_factor * data['vload_array'][:, 1, :]
    data['voc_array'][2, :] = data['voc_array'][2, :] - \
        temperature_correction_factor * data['voc_array'][1, :]
    data['isc_array'][2, :] = data['isc_array'][2, :] - \
        temperature_correction_factor * data['isc_array'][1, :]
    # TODO: lookup how voltage correction is applied at outside of 1 sun and verify implementation above

    return data


def interpolate_load_data(input_data):
    '''
    V, I, T, t, and G are measured for every flash point; however, there is 
    slight variation in exact values between every flash. This function makes 
    it so every point has the same exact intensity value assigned to it.
    '''
    data = input_data.copy()

    data['vload_array_interp'] = data['vload_array'].copy()
    data['voc_array_interp'] = data['voc_array'].copy()
    data['isc_array_interp'] = data['isc_array'].copy()

    # find highest and lowest intensity that is available for each pulse
    idx_max_intensity = np.zeros(
        [data['vload_number_of_load_conditions']+2], dtype=np.uint)
    min_intensity_array = np.zeros(
        [data['vload_number_of_load_conditions']+2], dtype=float)
    max_intensity_array = np.zeros(
        [data['vload_number_of_load_conditions']+2], dtype=float)

    vload_range = np.arange(
        data['vload_number_of_load_conditions'], dtype=np.uint16)
    for a in vload_range:
        idx_max_intensity[a] = (data['vload_array'][a, 1, :]).argmax()
        max_intensity_array[a] = np.max(
            data['vload_array'][a, 1, idx_max_intensity[a]:])
        min_intensity_array[a] = np.min(
            data['vload_array'][a, 1, idx_max_intensity[a]:])

    idx_max_intensity[a+1] = (data['voc_array'][1, :]).argmax()
    max_intensity_array[a +
                        1] = np.max(data['voc_array'][1, idx_max_intensity[a+1]:])
    min_intensity_array[a +
                        1] = np.min(data['voc_array'][1, idx_max_intensity[a+1]:])

    idx_max_intensity[a+2] = (data['isc_array'][1, :]).argmax()
    max_intensity_array[a +
                        2] = np.max(data['isc_array'][1, idx_max_intensity[a+2]:])
    min_intensity_array[a +
                        2] = np.min(data['isc_array'][1, idx_max_intensity[a+2]:])

    data['max_intensity'] = np.min(max_intensity_array)
    data['min_intensity'] = np.max(min_intensity_array)

    # Create new intensity array that is consistent for all load conditions
    # Takes min to max intensity and finds even intensity spacings equal to
    # number of voltage points obtained per flash.
    intensity_array = np.linspace(data['min_intensity'], data['max_intensity'],
                                  data['vload_number_of_points_per_flash'], dtype=float)

    # Interpolate all data based on the new intensity array
    for a in vload_range:
        data['vload_array_interp'][a, 0, :] = np.interp(intensity_array, np.flip(
            data['vload_array'][a, 1, idx_max_intensity[a]:], 0), np.flip(data['vload_array'][a, 0, idx_max_intensity[a]:], 0))
        data['vload_array_interp'][a, 1, :] = intensity_array
        data['vload_array_interp'][a, 2, :] = np.interp(intensity_array, np.flip(
            data['vload_array'][a, 1, idx_max_intensity[a]:], 0), np.flip(data['vload_array'][a, 2, idx_max_intensity[a]:], 0))
        data['vload_array_interp'][a, 3, :] = np.interp(intensity_array, np.flip(
            data['vload_array'][a, 1, idx_max_intensity[a]:], 0), np.flip(data['vload_array'][a, 3, idx_max_intensity[a]:], 0))
        data['vload_array_interp'][a, 4, :] = np.interp(intensity_array, np.flip(
            data['vload_array'][a, 1, idx_max_intensity[a]:], 0), np.flip(data['vload_array'][a, 4, idx_max_intensity[a]:], 0))

    data['voc_array_interp'][0, :] = np.interp(intensity_array, np.flip(
        data['voc_array'][1, idx_max_intensity[a+1]:], 0), np.flip(data['voc_array'][0, idx_max_intensity[a+1]:], 0))
    data['voc_array_interp'][1, :] = intensity_array
    data['voc_array_interp'][2, :] = np.interp(intensity_array, np.flip(
        data['voc_array'][1, idx_max_intensity[a+1]:], 0), np.flip(data['voc_array'][2, idx_max_intensity[a+1]:], 0))
    data['voc_array_interp'][3, :] = np.interp(intensity_array, np.flip(
        data['voc_array'][1, idx_max_intensity[a+1]:], 0), np.flip(data['voc_array'][3, idx_max_intensity[a+1]:], 0))
    data['voc_array_interp'][4, :] = np.interp(intensity_array, np.flip(
        data['voc_array'][1, idx_max_intensity[a+1]:], 0), np.flip(data['voc_array'][4, idx_max_intensity[a+1]:], 0))

    data['isc_array_interp'][0, :] = np.interp(intensity_array, np.flip(
        data['isc_array'][1, idx_max_intensity[a+2]:], 0), np.flip(data['isc_array'][0, idx_max_intensity[a+2]:], 0))
    data['isc_array_interp'][1, :] = intensity_array
    data['isc_array_interp'][2, :] = np.interp(intensity_array, np.flip(
        data['isc_array'][1, idx_max_intensity[a+2]:], 0), np.flip(data['isc_array'][2, idx_max_intensity[a+2]:], 0))
    data['isc_array_interp'][3, :] = np.interp(intensity_array, np.flip(
        data['isc_array'][1, idx_max_intensity[a+2]:], 0), np.flip(data['isc_array'][3, idx_max_intensity[a+2]:], 0))
    data['isc_array_interp'][4, :] = np.interp(intensity_array, np.flip(
        data['isc_array'][1, idx_max_intensity[a+2]:], 0), np.flip(data['isc_array'][4, idx_max_intensity[a+2]:], 0))
    data['intensity_array'] = intensity_array
    return data


def filter_iv_nans(data):
    # A nan value is stored as str in text file, which changes
    # all values to str
    if type(data.iloc[0, 0]) is not str:
        pass
    else:
        for idx, row in enumerate(data.iloc[:, 0]):
            if 'NaN' in row:
                data = data.drop(index=(idx))

            data = data.reset_index(drop=True).astype(float)
    return data


def extract_iv_data(data, suns=1):
    voltage = np.zeros(
        [data['vload_number_of_load_conditions']+2], dtype=float)
    current = np.zeros(
        [data['vload_number_of_load_conditions']+2], dtype=float)

    for a in np.arange(data['vload_number_of_load_conditions'], dtype=np.uint16):

        # suns=input intensity,data['vload_array_interp'][a,1,:]=measured intensity array, data['vload_array_interp'][a,2,:]=voltage)
        voltage[a] = np.interp(
            suns, data['vload_array_interp'][a, 1, :], data['vload_array_interp'][a, 2, :])
        current[a] = np.interp(
            suns, data['vload_array_interp'][a, 1, :], data['vload_array_interp'][a, 3, :])

    # Finding Voc
    voltage[a+1] = np.interp(suns, data['voc_array_interp']
                             [1, :], data['voc_array_interp'][2, :])
    current[a+1] = np.interp(suns, data['voc_array_interp']
                             [1, :], data['voc_array_interp'][3, :])

    # Finding Isc
    voltage[a+2] = np.interp(suns, data['isc_array_interp']
                             [1, :], data['isc_array_interp'][2, :])
    current[a+2] = np.interp(suns, data['isc_array_interp']
                             [1, :], data['isc_array_interp'][3, :])

    idx_sort = np.argsort(voltage)
    voltage = voltage[idx_sort]
    current = current[idx_sort]

    data['current'] = current
    data['voltage'] = voltage
    intensity = len(current)*[suns]
    iv_curve = pd.DataFrame(
        {'Voltage_(V)': voltage, 'Current_(A)': current, 'Intensity_(Suns)': intensity})

    iv_curve = filter_iv_nans(iv_curve)

    return data, voltage, current, iv_curve


def get_iv_intensity_array(data, step=1, sun=None):
    g = data['voc_array_interp'][1, :]

    if sun:
        g = np.array([g[g > sun-0.001][0]])
    else:
        g = g[::step]

    num_points = data['vload_number_of_load_conditions'] + 2
    ivg = np.zeros(shape=(num_points, len(g), 3))

    for i, intensity in enumerate(g):
        data, voltage, current, iv_curve = extract_iv_data(data, intensity)
        ivg[:, i, :] = iv_curve

    if not sun:
        ivg = ivg[:, :-1, :]

    return ivg


'''
# TODO fix later
def extract_reference_constant(data, method = 'power', reference_value = None, input_intensity = 1, tolerance = 0.00005):

    actual_value = 0
    step = 1/reference_value
    a = 0
    last_change = 'none'
    delta = reference_value-actual_value
    while abs(delta) > tolerance:
        performance = extract_performance_characteristics(data, suns = input_intensity)
        actual_value = performance['Isc']
        delta = reference_value-actual_value
        
        if delta > tolerance  and last_change == 'decrease':
            step = step/2
            input_intensity += step
            last_change = 'increase'
        elif delta < -tolerance and last_change == 'increase':
            step = step/2
            input_intensity -= step
            last_change = 'decrease'
        elif delta > tolerance:
            input_intensity += step
            last_change = 'increase'
        elif delta < -tolerance:
            input_intensity -= step
            last_change = 'decrease'
        
        a += 1
        
        if a == 1000:
            reference_constant = input_intensity*data['reference_constant'] 
            print('Maximum number of iterations reached. Reference constant at iteration 1000 = ' + str(reference_constant))
            break
        
    print('Number of iterations:', a)
    print('Intensity:', input_intensity)
    reference_constant = input_intensity*data['reference_constant']    
        
    return reference_constant, input_intensity
'''

'''
def exclude_data(input_data, time = 0.001):
    # removes data below the specified time value for the voc and isc arrays
    data = input_data.copy()
    
    #identify idices to exclude
    idx_excluded_voc = np.where(data['voc_array'][0,:] > time)  
    idx_excluded_isc = np.where(data['isc_array'][0,:] > time)
    
    #update the number of points
    data['voc_number_of_points_per_flash'] = len(idx_excluded_voc[0])    
    data['isc_number_of_points_per_flash'] = len(idx_excluded_isc[0])
    
    #update arrays
    data['voc_array'] = np.zeros([5,data['voc_number_of_points_per_flash']], dtype=float)
    data['isc_array'] = np.zeros([5,data['isc_number_of_points_per_flash']], dtype=float)
    
    for a in np.arange(5,dtype=np.uint16):
        data['voc_array'][a,:] = input_data['voc_array'][a,idx_excluded_voc]
        data['isc_array'][a,:] = input_data['isc_array'][a,idx_excluded_isc]
      
    return data
'''


def get_suns_voc(data, step=1):
    suns = data['voc_array'][1, :][::-1]
    voc = data['voc_array'][2, :]
    voc.sort()
    suns_voc = np.vstack((suns, voc)).T

    suns_voc = suns_voc[::step]
    suns_voc = suns_voc[:-1, :]

    return suns_voc


def get_piv_intensity_array(suns_voc, ivg, sun=None):
    g_array = ivg[0, :, 2]
    g_len = len(g_array)
    # np.zeros(shape=(g_len,g_len,3))
    pivg = np.zeros(shape=(len(suns_voc[:, 0]), g_len, 3))

    for a, g in enumerate(g_array):
        isc = np.max(ivg[:, a, 1])

        pseudo_i = -1*suns_voc[:, 0]*isc + isc
        pseudo_v = suns_voc[:, 1]
        intensity = len(pseudo_i)*[g]  # g_len*[g]
        piv_curve = pd.DataFrame({'pseudo-Voltage_(V)': pseudo_v,
                                  'pseudo-Current_(A)': pseudo_i,
                                  'Intensity_(Suns)': intensity})

        pivg[:, a, :] = piv_curve

    return pivg


def extract_parameter_v_intensity(data, rsh_v_cell=0.45):
    # Module information for parameters
    num_cells_per_str = data['number_of_cells_per_string']
    num_cells = num_cells_per_str*data['number_of_strings']
    active_area = num_cells*data['cell_area']/10000
    module_area = data['module_area']/10000

    # Get I-V(G) and pI-V(G) for I-V and pI-V curve extraction
    ivg = data['iv_curve_intensity']
    pivg = data['pseudo-iv_curve_intensity']

    g_array = ivg[0, :, 2]
    # Initializing parameter v intensity keys
    parameters = ['pmp', 'imp', 'vmp', 'voc', 'isc', 'ff', 'module_efficiency',
                  'active_area_efficiency', 'rs', 'rsh', 'pseudo-pmp', 'pseudo-imp',
                  'pseudo-vmp', 'pseudo-voc', 'pseudo-isc', 'pseudo-ff',
                  'pseudo-module_efficiency', 'pseudo-active_area_efficiency']

    for parameter in parameters:
        data[f"intensity_{parameter}"] = np.zeros(len(g_array))

    for a, g in enumerate(g_array):
        # Get pseudo and actual I-V at intensity g
        #piv = pivg[:,a,:]
        iv = ivg[:, a, :]
        piv = pivg[:, a, :]

        # Getting voltage and current
        voltage = iv[:, 0]
        current = iv[:, 1]

        # Parameters saved for interpolation method
        voc = np.max(voltage)
        isc = np.max(current)

        # Polynomial fit of I-V curve for other parameters
        coeffs = poly.polyfit(voltage, current, 10)  # 10
        voltage = np.linspace(voltage[0], voltage[-1], num=5000)
        # these are the new current values
        current = poly.polyval(voltage, coeffs)
        voltage = voltage[np.argsort(voltage)]
        current = current[np.argsort(voltage)]

        # extracted from polyfit
        power = voltage*current
        pmp = np.max(power)
        vmp = voltage[(power).argmax()]
        imp = current[(power).argmax()]
        ff = (vmp*imp)/(voc*isc)

        module_efficiency = (100*pmp)/(g*1000*module_area)
        active_area_efficiency = (100*pmp)/(1000*g)/active_area

        # Calculate Rsh
        # 0.45V/cell is a Sinton tool default
        pv_filter = num_cells_per_str*rsh_v_cell

        piv_filtered = piv[piv[:, 0] < pv_filter]
        dpv = piv_filtered[-1, 0] - piv_filtered[0, 0]
        dpi = piv_filtered[-1, 1] - piv_filtered[0, 1]
        rsh = -dpv / dpi

        # Get pseudo parameters
        # Attempted polyfit on pI-V and got same results
        ppower = piv[:, 0]*piv[:, 1]
        ppmp = np.max(ppower)
        pimp = piv[ppower.argmax(), 1]
        pvmp = piv[ppower.argmax(), 0]

        pvoc = np.max(piv[:, 0])
        pisc = np.max(piv[:, 1])
        pff = ppmp/(pvoc*pisc)

        pmodule_efficiency = ppmp/(1000*g)/module_area
        pactive_area_efficiency = ppmp/(1000*g)/active_area

        # Calculate Rs from pI-V and I-V
        pv_at_imp = piv[piv[:, 1] < imp][0, 0]
        rs = (pv_at_imp - vmp) / imp

        parameters_intensity = [pmp, imp, vmp, voc, isc, ff, module_efficiency,
                                active_area_efficiency, rs, rsh, ppmp, pimp, pvmp,
                                pvoc, pisc, pff, pmodule_efficiency,
                                pactive_area_efficiency]
        for parameter, parameter_intensity in zip(parameters, parameters_intensity):
            data[f"intensity_{parameter}"][a] = parameter_intensity

    return data


# # Not giving the right numbers
# def get_j0_g(v_g, i_g, area, temperature):
#     """
#     Parameters
#     ----------
#     voc : TYPE
#         DESCRIPTION.
#     isc : TYPE
#         DESCRIPTION.
#     area : TYPE
#         DESCRIPTION.
#     temperature : TYPE
#         DESCRIPTION.

#     Returns
#     -------
#     j0_g : TYPE
#         DESCRIPTION.

#     """

#     # Defining constants
#     q = 1.602176634e-19
#     k = 8.617e-5 #1.380649e-23

#     j_g = i_g/area
#     resistance_param = (v_g - i_g*rs)/rsh
#     exponential = (np.exp((v_g*q) / (k*temperature))-1)
#     j0_g = ((i_g - resistance_param)/area) / exponential

#     return j0_g

# temperature = data['temperature'] + 273.15
# area = data['cell_area']
# data['intensity_j0'] = np.zeros(len(data['intensity_array']))
# for idx, g in enumerate(data['intensity_array']):
#     v_g = data['intensity_vmp'][idx]
#     i_g = data['intensity_imp'][idx]
#     rs = data['intensity_rs'][idx]
#     rsh = data['intensity_rsh'][idx]
#     data['intensity_j0'][idx] = get_j0(v_g, i_g, area, temperature)


def iv_analysis(mfr_file, reference_constant=None,
                voltage_temperature_coefficient=None,
                rsh_v_cell=0.45, step=1, sun=None):
    '''
    \n The analysis performs the following steps:
        1. Imports and reads the data
        2. Corrects the data with parameters in data file. The Sinton software already
        corrects the data, but this function is in case the user would like to 
        correct to different parameters, such as reference_constant, 
        voltage_temperature_coefficient, or temperature_offset.
        3. Interpolates V, Voc, and Isc for number of data points obtained per
        flash (500).
        4. Interpolates again or uses polynomial fit to get I-V data
        5. Extracts performance data: Pmp, Vmp, Imp, Voc, Isc, Efficiency, 
        FF, I-V data
    '''
    # Load data and recipe file data from mfr file.
    data, mfi_contents = import_raw_data_from_file(mfr_file)

    # Correct data based on correction constants
    corrected_data = correct_raw_data(data, reference_constant=reference_constant,
                                      voltage_temperature_coefficient=voltage_temperature_coefficient)

    # Interpolation necessary to account for slight differences in intensity
    # values at each flash point.
    corrected_data = interpolate_load_data(corrected_data)

    # Extracting I-V and pseudo-I-V data
    corrected_data['iv_curve_intensity'] = get_iv_intensity_array(corrected_data,
                                                                  step, sun)
    corrected_data['suns_voc'] = get_suns_voc(corrected_data, step)
    corrected_data['pseudo-iv_curve_intensity'] = get_piv_intensity_array(
        corrected_data['suns_voc'],
        corrected_data['iv_curve_intensity'])

    corrected_data['intensity_array'] = corrected_data['iv_curve_intensity'][0, :, 2]

    # Extracting performance versus intensity data
    corrected_data = extract_parameter_v_intensity(corrected_data, rsh_v_cell)

    return corrected_data, mfi_contents


def get_intensity_idx(iv_data, intensity=1):
    '''
    Finds the index of the I-V and p-I-V curves for the nearest intensity
    value defined by the user.
    '''
    intensity_array = iv_data['iv_curve_intensity'][0, :, 2]
    intensity_array = intensity_array[intensity_array < intensity + 0.001]
    intensity_idx = len(intensity_array) - 1
    if intensity_idx < 0:
        raise ValueError('This intensity is too low.')
    else:
        intensity_actual = intensity_array[-1]

    return intensity_idx, intensity_actual


def bad_curve_detector_txt(files):
    #files = fmgmt.get_files()
    for file in files:
        # Get folder and filename information
        src = f"{os.path.dirname(files[0])}/"
        ext = file.split('.')[-1]

        # This allows the user to load all files without worrying about extensions
        if ext == 'mfr':
            mfr_file = file
            txt_file = file.replace('mfr', 'txt').replace('IVT20', '20')
        elif ext == 'txt':
            txt_file = file
            mfr_file = file.replace(
                basename(file), 'IVT'+basename(file)).replace('txt', 'mfr')

        iv_data = pd.read_csv(txt_file, sep='\t')
        #iv_data, mfi_contents = iv_analysis(file)
        # Unclear if we need to filter for one-sun data, but playing it safe
        onesun_idx, intensity_actual = get_intensity_idx(iv_data, intensity=1)
        iv_data = pd.DataFrame(
            iv_data['iv_curve_intensity'][:, onesun_idx, :2])

        # Filters
        iv_data = iv_data[iv_data.iloc[:, 0] != iv_data.iloc[:, 1]]
        iv_data = iv_data.iloc[:, :2]

        power = iv_data.iloc[:, 0]*iv_data.iloc[:, 1]
        vmp = iv_data.iloc[power.argmax(), 0]
        imp = iv_data.iloc[power.argmax(), 1]

        #date = basename(file).split('_')[0]
        #time = basename(file).split('_')[3]
        #plt.plot(iv_data.iloc[:,0],iv_data.iloc[:,1],label=f"{date} {time}")
        # plt.legend(bbox_to_anchor=(1,1))

        # Start with assumption of good curve
        bad = False

        # Check: if first voltage point is negative /very low - module not connected
        if iv_data.iloc[0, 0] < 1:
            print('First V is negative/very low')
            bad = True

        # Check: if any current values are negative
        if iv_data.iloc[:, 1].min() < 0:
            print('Negative current values found.')
            bad = True

        # Check: if voltage is too high or too low
        if (iv_data.iloc[:, 0].max() > 80) or (iv_data.iloc[:, 0].min() < -5):
            print('Max voltage is too high/too low.')
            bad = True

        # Check: length of points in curve
        if len(iv_data) < 12:
            print('IV curve stopped early.')
            bad = True

        # Check: current values at V < Vmp are below Imp
        if iv_data[iv_data.iloc[:, 0] < vmp].iloc[:, 1].min() < 0.95*imp:
            print('At least one point at V<Vmp shows I<0.95*Imp.')
            bad = True

        # Check: Isc is really low
        if iv_data.iloc[:, 1].max() < 2:
            print('Isc is too low.')
            bad = True

        # If bad file, move to a check folder to verify before deleting.
        if bad:
            print(basename(file))
            # If a bad file appears to exist, it will create a folder for inspection.
            check_folder = f"{src}c/"
            if not os.path.isdir(check_folder):
                os.mkdir(check_folder)
            if os.path.isfile(mfr_file):
                fmgmt.copy_files([mfr_file], f"{check_folder}")
                os.remove(mfr_file)
            if os.path.isfile(txt_file):
                fmgmt.copy_files([txt_file], f"{check_folder}")
                os.remove(txt_file)


def bad_curve_detector(iv_data, isc_override=False):
    '''
    typically a try, except is used for the mfr files to tell if they are bad;
    however, some curves can be read properly but still have bad data
    '''

    # Unclear if we need to filter for one-sun data, but playing it safe
    if len(iv_data['intensity_array']) > 1:
        onesun_idx, intensity_actual = get_intensity_idx(iv_data, intensity=1)
        iv_data = pd.DataFrame(
            iv_data['iv_curve_intensity'][:, onesun_idx, :2])
    else:
        iv_data = pd.DataFrame(iv_data['iv_curve_intensity'][:, 0, :2])

    power = iv_data.iloc[:, 0]*iv_data.iloc[:, 1]
    vmp = iv_data.iloc[power.argmax(), 0]
    imp = iv_data.iloc[power.argmax(), 1]

    # Start with assumption of good curve
    bad = False

    # Check: if first voltage point is negative /very low - module not connected
    if iv_data.iloc[0, 0] < 1:
        print('First V is negative/very low')
        bad = True

    # Check: if any current values are substantially negative
    # There will often be a very slight negative current at Voc on the order
    # of microamps. Likelye noise but it does get recorded.
    if iv_data.iloc[:, 1].min() < -0.01:
        print('Negative current values found.')
        bad = True

    # Check: if voltage is too high or too low
    if (iv_data.iloc[:, 0].max() > 80) or (iv_data.iloc[:, 0].min() < -5):
        print('Max voltage is too high/too low.')
        bad = True

    # Check: length of points in curve
    if len(iv_data) < 12:
        print('IV curve stopped early.')
        bad = True

    # Check: current values at V < Vmp are below Imp
    if iv_data[iv_data.iloc[:, 0] < vmp].iloc[:, 1].min() < 0.95*imp:
        print('At least one point at V<Vmp shows I<0.95*Imp.')
        bad = True

    # Check: Isc is really low
    if not isc_override and (iv_data.iloc[:, 1].max() < 2):
        print('Isc is too low.')
        bad = True

    return bad
    '''
    # If bad file, move to a check folder to verify before deleting.
    if bad:
        print(basename(file))
        # If a bad file appears to exist, it will create a folder for inspection.
        check_folder = f"{src}c/"
        if not os.path.isdir(check_folder):
            os.mkdir(check_folder)
        if os.path.isfile(mfr_file):
            fmgmt.copy_files([mfr_file],f"{check_folder}")
            os.remove(mfr_file)
        if os.path.isfile(txt_file):
            fmgmt.copy_files([txt_file],f"{check_folder}")
            os.remove(txt_file)
    '''


def import_suns_voc_data(text_file):
    all_data = pd.read_csv(text_file, sep='\t')

    # Different versions of software save the files with a space versus an
    # underscore in the RsLoad header because why not?
    try:
        iv_rs_data = all_data.loc[:, 'Vload_(V)':'RsLoad (ohm)']
    except:
        iv_rs_data = all_data.loc[:, 'Vload_(V)':'RsLoad_(ohm)']

    # >0 filters out empty entries
    iv_rs_data = iv_rs_data[iv_rs_data.loc[:, 'Vload_(V)'] > 0]

    # in the rare case that the Pmp flash is bad, it will get filtered out and
    # can lead to KeyErrors when finding Vmp
    iv_rs_data = iv_rs_data.reset_index(drop=True)

    pseudo_iv_data = all_data.loc[:,
                                  'SunsVoc_Voltage_(V)':'SunsVoc_Current_(A)']
    iv_fit_data = all_data.loc[:, 'Model_Voltage_(V)':'Model_Current_(A)']
    lifetime_injection_data = all_data.loc[:,
                                           'Carrier_Density_(cm-3)':'Inverse_Lifetime_(s-1)']
    efficiency_intensity_data = all_data.loc[:,
                                             'Efficiency_(%)':'Intensity_(suns)']

    text_file_summary = {'iv_rs_data': iv_rs_data, 'pseudo_iv_data': pseudo_iv_data,
                         'iv_fit_data': iv_fit_data,
                         'lifetime_injection_data': lifetime_injection_data,
                         'efficiency_intensity_data': efficiency_intensity_data}
    return text_file_summary


def get_lifetime_at_vmp(lifetime_injection_data, iv_fit_data, vmp):
    # Find indices such that voltage is > Vmp - 0.2
    lifetime_idxs = iv_fit_data[iv_fit_data['Model_Voltage_(V)'] > vmp-0.2]
    # Use the last three data points
    lifetime_idxs = lifetime_idxs[lifetime_idxs['Model_Voltage_(V)'] < vmp+0.2]
    lifetime_idxs = list(lifetime_idxs.index)
    lifetime_vmp = np.mean(
        lifetime_injection_data.loc[lifetime_idxs, 'Lifetime_(s)'])

    return lifetime_vmp


def check_iv_curve(mfr_file, step=1, sun=None, override=False, isc_override=False):
    '''
    Parameters
    ----------
    mfr_file : str
        Sinton I-V results filepath.

    Returns
    -------
    bad : bool
        True if the curve fails the check; False if it passes.
    iv_data : dict
        iv_analysis output with the curve fully analyzed.
    mfi_contents : list
        List of lines from the mfi recipe file contents that are within the
        mfr file.

    '''

    bad = False
    iv_data = None
    mfi_contents = None
    # First check - if error is thrown even reading the file, bad data
    try:
        # First check sees if file can be read
        iv_data, mfi_contents = import_raw_data_from_file(mfr_file)

        if override:
            iv_data, mfi_contents = iv_analysis(mfr_file, reference_constant=None,
                                                voltage_temperature_coefficient=None,
                                                rsh_v_cell=0.45, step=step,
                                                sun=sun)
            bad = False

        elif iv_data and not override:
            try:
                # Second check - data can be read but may still be bad
                # Need to run full analysis to get iv_data
                iv_data, mfi_contents = iv_analysis(mfr_file, reference_constant=None,
                                                    voltage_temperature_coefficient=None,
                                                    rsh_v_cell=0.45, step=step,
                                                    sun=sun)
                # TODO fix later to account for high voltage modules
                if 'FIRSTSOLAR' in mfr_file:
                    bad = False
                    pass
                else:
                    bad = bad_curve_detector(iv_data, isc_override)
            except:
                bad = True
                print(f"Bad curve: {basename(mfr_file)}.")

        if bad:
            print(f"Bad curve: {basename(mfr_file)}.")

    except:
        bad = True
        print(f"Bad curve: {basename(mfr_file)}. Cannot read file.")

    return bad, iv_data, mfi_contents


if __name__ == "__main__":
    mfr_file = sys.argv[1]
    bad, iv_data, mfi_contents = check_iv_curve(
        mfr_file, step=1, sun=None, override=True, isc_override=False)
    print(f"Curve is bad: {bad}.")
